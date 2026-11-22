package application

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/SolaTyolo/17track-wrapper/internal/domain"
	"github.com/SolaTyolo/17track-wrapper/pkg/track17"
)

const (
	idempTTLSeconds = 24 * 3600
	idempKeyPrefix  = "tracking:cb:"
	archiveLockName = "tracking:archive"

	// DefaultArchiveRetentionDays: inactive packages untouched this long move to cold storage.
	DefaultArchiveRetentionDays = 100
)

// Service owns package storage, 17TRACK subscription and archive.
// 17TRACK uses a single platform account from TRACK17_SECRET (see track17.NewClientFromEnv).
type Service struct {
	repo           domain.Repository
	sdk            track17.Client
	registerQ      domain.RegisterJobPublisher
	idemp          domain.IdempotencyStore
	cold           domain.ColdStore
	retention      time.Duration
	archiveBatch   int
	syncStaleAfter time.Duration
	observers      []Observer
}

type Options struct {
	// ArchiveRetentionDays: days without updates before an inactive package is archived.
	ArchiveRetentionDays int
	ArchiveBatchSize     int
	// SyncStaleAfter: SyncActive only polls TRACKING packages with no 17TRACK data for this long.
	SyncStaleAfter time.Duration
}

func NewService(
	repo domain.Repository,
	sdkClient track17.Client,
	idemp domain.IdempotencyStore,
	cold domain.ColdStore,
	opts Options,
) *Service {
	retentionDays := opts.ArchiveRetentionDays
	if retentionDays <= 0 {
		retentionDays = DefaultArchiveRetentionDays
	}
	batch := opts.ArchiveBatchSize
	if batch <= 0 {
		batch = 200
	}
	syncStale := opts.SyncStaleAfter
	if syncStale <= 0 {
		syncStale = 48 * time.Hour
	}
	return &Service{
		repo:           repo,
		sdk:            sdkClient,
		idemp:          idemp,
		cold:           cold,
		retention:      time.Duration(retentionDays) * 24 * time.Hour,
		archiveBatch:   batch,
		syncStaleAfter: syncStale,
	}
}

// WithRegisterQueue enables deferred register retries when 17TRACK rate-limits.
func (s *Service) WithRegisterQueue(q domain.RegisterJobPublisher) *Service {
	if s != nil {
		s.registerQ = q
	}
	return s
}

// client returns the platform 17TRACK client (TRACK17_SECRET; Selective when ENABLE_MOCK).
func (s *Service) client() (track17.Client, error) {
	if s.sdk == nil {
		return nil, domain.ErrNotConfigured
	}
	// Empty token is OK only for ENABLE_MOCK nop-carrier local (Selective.Token is non-empty).
	if strings.TrimSpace(s.sdk.Token()) == "" {
		return nil, domain.ErrNotConfigured
	}
	return s.sdk, nil
}

// locator loads and row-locks one package inside a transaction.
type locator func(context.Context, domain.Repository) (*domain.Package, error)

// byNumber locks the current generation of tn.
func byNumber(tn string) locator {
	return func(ctx context.Context, r domain.Repository) (*domain.Package, error) {
		return r.GetPackageForUpdate(ctx, tn)
	}
}

// byID locks one specific generation.
func byID(id int64) locator {
	return func(ctx context.Context, r domain.Repository) (*domain.Package, error) {
		return r.GetPackageByIDForUpdate(ctx, id)
	}
}

type ingestOpts struct {
	// accept gates the whole write after the row is locked; nil accepts every package.
	accept func(*domain.Package) bool
	// after runs on the locked package once the batch is applied, before the single UPDATE.
	after func(*domain.Package)
	// publish builds the observer update for a notable write. It is delivered inside the
	// transaction: with change-only notifications a failed delivery must roll the write
	// back, or the caller's retry (17TRACK redelivery / next poll) would see no change.
	publish func(ingestResult) Update
}

type ingestResult struct {
	// Package is the locked row: written when Applied, as read otherwise.
	Package  *domain.Package
	Applied  bool
	Inserted []domain.TrackingEvent
	// Changed: status, sub_status, subscription, carrier or return type moved.
	Changed bool
}

// Notable reports whether observers should hear about this write.
func (r ingestResult) Notable() bool { return r.Applied && (r.Changed || len(r.Inserted) > 0) }

type notableState struct {
	status, subStatus, subscription, returnType string
	carrier                                     int
}

func stateOf(p *domain.Package) notableState {
	st := notableState{status: p.Status, subStatus: p.SubStatus, subscription: p.SubscriptionStatus, carrier: p.Carrier}
	if p.ReturnType != nil {
		st.returnType = *p.ReturnType
	}
	return st
}

// ingest locks the package row, applies batch (may be nil) and opts.after, and writes
// the package once in the same transaction. Archived packages take no events: restore first.
func (s *Service) ingest(ctx context.Context, find locator, batch *domain.EventBatch, opts ingestOpts) (ingestResult, error) {
	var res ingestResult
	err := s.repo.WithTx(ctx, func(tx domain.Repository) error {
		pkg, err := find(ctx, tx)
		if err != nil {
			return err
		}
		res = ingestResult{Package: pkg}
		if opts.accept != nil && !opts.accept(pkg) {
			return nil
		}
		if batch != nil && pkg.IsArchived {
			return fmt.Errorf("%w: package %d is archived; restore it first", domain.ErrConflict, pkg.PackageID)
		}
		res.Applied = true
		before := stateOf(pkg)
		if batch != nil {
			events := make([]domain.TrackingEvent, len(batch.Events))
			for i, ev := range batch.Events {
				ev.PackageID = pkg.PackageID
				ev.TrackingNumber = pkg.TrackingNumber
				if ev.EventFingerprint == "" {
					ev.EventFingerprint = domain.Fingerprint(ev.TrackingNumber, ev.EventTime, ev.Location, ev.Description, ev.Stage)
				}
				events[i] = ev
			}
			if res.Inserted, err = tx.InsertEvents(ctx, events); err != nil {
				return err
			}
			stale := pkg.LatestUpdateAt != nil && !batch.UpdateTime.IsZero() && batch.UpdateTime.Before(*pkg.LatestUpdateAt)
			if !stale {
				applyBatch(pkg, *batch)
			}
		}
		if opts.after != nil {
			opts.after(pkg)
		}
		res.Changed = stateOf(pkg) != before
		if err := tx.UpdatePackage(ctx, pkg); err != nil {
			return err
		}
		if opts.publish != nil && res.Notable() {
			return s.notify(ctx, opts.publish(res))
		}
		return nil
	})
	if err != nil {
		return ingestResult{}, err
	}
	return res, nil
}

// update re-reads the package under row lock and writes apply's changes when accept
// (nil accepts all) holds on the fresh row. Every write that follows a 17TRACK call or a
// paged read must go through here so concurrent webhook ingests are not overwritten.
func (s *Service) update(ctx context.Context, find locator, accept func(*domain.Package) bool, apply func(*domain.Package)) (*domain.Package, bool, error) {
	res, err := s.ingest(ctx, find, nil, ingestOpts{accept: accept, after: apply})
	return res.Package, res.Applied, err
}

func applyBatch(pkg *domain.Package, batch domain.EventBatch) {
	if batch.Status != "" {
		pkg.Status = batch.Status
		pkg.SubStatus = batch.SubStatus
		pkg.SubStatusDesc = batch.SubStatusDesc
	}
	if batch.Weight != nil {
		pkg.Weight = batch.Weight
	}
	pkg.Merge(batch.ShippingDetail)

	merged := domain.Milestones{}
	for _, ev := range batch.Events {
		if ev.Stage == "" {
			continue
		}
		if cur, ok := merged[ev.Stage]; !ok || ev.EventTime.Before(cur) {
			merged[ev.Stage] = ev.EventTime
		}
	}
	for stage, at := range pkg.Milestones {
		merged[stage] = at
	}
	for stage, at := range batch.Milestones {
		merged[stage] = at
	}
	if len(merged) > 0 {
		pkg.Milestones = merged
	}
	if at, ok := pkg.Milestones[domain.StagePickedUp]; ok {
		pkg.PickedUpAt = &at
	}
	if at, ok := pkg.Milestones[domain.StageDelivered]; ok {
		pkg.DeliveredAt = &at
		if batch.Status == "" && pkg.Status != domain.StatusDelivered {
			pkg.Status = domain.StatusDelivered
			pkg.SubStatus = ""
			pkg.SubStatusDesc = ""
		}
	}
	for _, ev := range batch.Events {
		if pkg.LatestEventTime == nil || ev.EventTime.After(*pkg.LatestEventTime) {
			et := ev.EventTime
			pkg.LatestEventTime = &et
			pkg.LatestEventDesc = ev.Description
			pkg.LatestEventLocation = ev.Location
		}
	}
	if pkg.Status == domain.StatusDelivered && pkg.DeliveredAt == nil {
		at := time.Now().UTC()
		if pkg.LatestEventTime != nil {
			at = *pkg.LatestEventTime
		} else if !batch.UpdateTime.IsZero() {
			at = batch.UpdateTime
		}
		pkg.DeliveredAt = &at
	}
	if !batch.UpdateTime.IsZero() {
		ut := batch.UpdateTime
		pkg.LatestUpdateAt = &ut
	}
}

// ApplyWebhookBody applies a raw 17TRACK push body ({event, data:{number,…}}) as relayed
// by the gateway, over HTTP or the event bus.
func (s *Service) ApplyWebhookBody(ctx context.Context, raw []byte) error {
	var body struct {
		Event       string          `json:"event"`
		Data        json.RawMessage `json:"data"`
		Number      string          `json:"number"`
		CallbackSeq string          `json:"callback_seq"`
		EventID     string          `json:"event_id"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return fmt.Errorf("%w: webhook body: %v", domain.ErrInvalid, err)
	}
	data := body.Data
	if len(data) == 0 && body.Event == "" {
		data = raw
	}
	return s.ApplyWebhook(ctx, domain.WebhookApplyInput{
		TrackingNo:  body.Number,
		CallbackSeq: firstNonEmpty(body.CallbackSeq, body.EventID),
		Raw:         data,
		Event:       body.Event,
	})
}

// ApplyWebhook handles 17TRACK pushes: ingest events + subscription updates.
func (s *Service) ApplyWebhook(ctx context.Context, in domain.WebhookApplyInput) error {
	tn := strings.TrimSpace(in.TrackingNo)
	event := strings.TrimSpace(in.Event)
	if event == "" {
		event = domain.EventTrackingUpdated
	}
	if event != domain.EventTrackingUpdated && event != domain.EventTrackingStopped {
		return fmt.Errorf("%w: unsupported event %s", domain.ErrInvalid, event)
	}
	hasData := hasDetail(in.Raw)
	var parsed track17.ParsedTrack
	if hasData {
		p, err := track17.ParseDetailJSON(in.Raw)
		if err != nil {
			return err
		}
		parsed = p
	}
	if tn == "" {
		var meta struct {
			Number string `json:"number"`
		}
		_ = json.Unmarshal(in.Raw, &meta)
		tn = strings.TrimSpace(meta.Number)
	}
	if tn == "" {
		return fmt.Errorf("%w: number required", domain.ErrInvalid)
	}

	seq := strings.TrimSpace(in.CallbackSeq)
	if seq == "" {
		seq = hashFallbackSeq(tn, in.Raw)
	}
	idempKey := idempKeyPrefix + event + ":" + seq
	if s.idemp != nil {
		claimed, err := s.idemp.Claim(ctx, idempKey, idempTTLSeconds)
		if err != nil {
			return err
		}
		if !claimed {
			return nil
		}
	}
	if err := s.applyWebhook(ctx, tn, event, hasData, parsed); err != nil {
		if s.idemp != nil {
			_ = s.idemp.Release(context.WithoutCancel(ctx), idempKey)
		}
		return err
	}
	return nil
}

func (s *Service) applyWebhook(ctx context.Context, tn, event string, hasData bool, parsed track17.ParsedTrack) error {
	var batch *domain.EventBatch
	if hasData {
		b := batchFromParsed(tn, parsed)
		batch = &b
	}
	now := time.Now().UTC()
	_, err := s.ingest(ctx, byNumber(tn), batch, ingestOpts{
		accept: func(p *domain.Package) bool { return p.IsActive() },
		after: func(p *domain.Package) {
			p.SubscriptionStatus = domain.SubTracking
			if batch != nil {
				applyReturnToPackage(p, batch.Events, now)
			}
			// 17TRACK webhook TrackDetail.carrier is the resolved key (auto-detect / correct carrier).
			applyResolvedCarrier(p, parsed.Carrier)
			if event == domain.EventTrackingStopped {
				p.SubscriptionStatus = domain.SubStopped
			}
			p.LastCallbackAt = &now
		},
		publish: func(r ingestResult) Update {
			u := Update{Package: *r.Package, NewEvents: r.Inserted, Pushed: true}
			if hasData {
				u.Parsed = &parsed
			}
			return u
		},
	})
	if errors.Is(err, domain.ErrNotFound) {
		return nil
	}
	return err
}

func batchFromParsed(tn string, parsed track17.ParsedTrack) domain.EventBatch {
	status := domain.MapSDKStatus(parsed.Status, parsed.SubStatus)
	batch := domain.EventBatch{
		TrackingNumber: tn,
		Status:         status,
		SubStatus:      parsed.SubStatus,
		SubStatusDesc:  firstNonEmpty(parsed.SubStatusDesc, subStatusDescr(parsed.SubStatus)),
		Weight:         parsed.WeightKG,
		ShippingDetail: domain.ShippingDetail{
			Origin: domain.Address{
				Country:    parsed.OriginCountry,
				State:      parsed.OriginState,
				City:       parsed.OriginCity,
				PostalCode: parsed.OriginPostalCode,
			},
			Destination: domain.Address{
				Country:    parsed.DestinationCountry,
				State:      parsed.DestinationState,
				City:       parsed.DestinationCity,
				PostalCode: parsed.DestinationPostal,
			},
			ServiceType:            parsed.ServiceType,
			Pieces:                 parsed.Pieces,
			ReferenceNumber:        parsed.ReferenceNumber,
			LastMileTrackingNumber: parsed.LocalNumber,
			LastMileCarrierCode:    parsed.LocalProvider,
		},
	}
	if t := parseEventTime(parsed.EstimatedDeliveryFrom); t != nil {
		batch.ShippingDetail.EstimatedDeliveryFrom = t
	}
	if t := parseEventTime(parsed.EstimatedDeliveryTo); t != nil {
		batch.ShippingDetail.EstimatedDeliveryTo = t
	}
	if t := parseEventTime(parsed.LastEventAt); t != nil {
		batch.UpdateTime = *t // already UTC; from latest_event.time_utc only
	}
	milestones := domain.Milestones{}
	for key, ts := range parsed.Milestones {
		if t := parseEventTime(ts); t != nil {
			milestones[key] = *t
		}
	}
	if parsed.PickupAt != "" {
		if t, err := time.Parse(time.RFC3339, parsed.PickupAt); err == nil {
			milestones[domain.StagePickedUp] = t
		}
	}
	if parsed.DeliveredAt != "" {
		if t, err := time.Parse(time.RFC3339, parsed.DeliveredAt); err == nil {
			milestones[domain.StageDelivered] = t
		}
	}
	batch.Milestones = milestones

	for _, ev := range parsed.Events {
		// Prefer time_utc (absolute); fall back to time_iso only if UTC missing.
		et := parseEventTime(firstNonEmpty(ev.TimeUTC, ev.TimeISO))
		// Untimed events fingerprint on the zero time so redeliveries dedupe; event_time
		// then records when the line was first seen.
		var fpTime time.Time
		if et != nil {
			fpTime = *et
		} else {
			now := time.Now().UTC()
			et = &now
		}
		te := domain.TrackingEvent{
			TrackingNumber: tn,
			EventTime:      *et,
			Stage:          ev.Stage,
			SubStatus:      ev.SubStatus,
			Description:    ev.Description,
			Location:       ev.Location,
			Address: domain.Address{
				Country:    ev.Country,
				State:      ev.State,
				City:       ev.City,
				PostalCode: ev.PostalCode,
			},
			Latitude:  ev.Lat,
			Longitude: ev.Lng,
		}
		te.EventFingerprint = domain.Fingerprint(tn, fpTime, te.Location, te.Description, te.Stage)
		batch.Events = append(batch.Events, te)
	}
	return batch
}

func subStatusDescr(sub string) string {
	return domain.SubStatusDescription(strings.TrimSpace(sub))
}

// applyReturnToPackage records the latest return hit. Package status stays the
// 17TRACK latest_status; return state lives only in the return_* columns.
func applyReturnToPackage(pkg *domain.Package, events []domain.TrackingEvent, now time.Time) {
	if len(events) == 0 {
		return
	}
	hit := domain.DetectReturn(events)
	pkg.ReturnDetectSupported = hit.Supported
	if hit.Type == "" {
		return
	}
	if pkg.ReturnType != nil && *pkg.ReturnType == hit.Type {
		return
	}
	t := hit.Type
	id := hit.CarrierEventID
	pkg.HasReturn = true
	pkg.ReturnType = &t
	pkg.ReturnCarrierEventID = &id
	pkg.ReturnDetectedAt = &now
}

// applyResolvedCarrier stores the carrier 17TRACK actually tracks with (auto-detect
// result or a carrier correction on their side).
func applyResolvedCarrier(pkg *domain.Package, carrier int) {
	if pkg != nil && carrier > 0 {
		pkg.Carrier = carrier
	}
}

// hashFallbackSeq keys seq-less pushes by content so redeliveries of the same body dedupe.
func hashFallbackSeq(tn string, raw json.RawMessage) string {
	sum := sha256.Sum256(raw)
	return tn + "-" + hex.EncodeToString(sum[:8])
}

func firstNonEmpty(vs ...string) string {
	for _, v := range vs {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func parseEventTime(v string) *time.Time {
	v = strings.TrimSpace(v)
	if v == "" {
		return nil
	}
	for _, layout := range []string{time.RFC3339, "2006-01-02T15:04:05", "2006-01-02 15:04:05", "2006-01-02"} {
		if t, err := time.Parse(layout, v); err == nil {
			t = t.UTC()
			return &t
		}
	}
	return nil
}

func hasDetail(raw json.RawMessage) bool {
	s := strings.TrimSpace(string(raw))
	return s != "" && s != "null" && s != "{}"
}
