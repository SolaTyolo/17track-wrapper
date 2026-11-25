package infrastructure

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SolaTyolo/17track-wrapper/internal/domain"
	"github.com/SolaTyolo/17track-wrapper/pkg/pgerr"
)

type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

// PostgresRepository implements domain.Repository.
type PostgresRepository struct {
	pool *pgxpool.Pool
	db   querier
	inTx bool
}

func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool, db: pool}
}

var _ domain.Repository = (*PostgresRepository)(nil)

func (r *PostgresRepository) WithTx(ctx context.Context, fn func(domain.Repository) error) error {
	if r.inTx {
		return fn(r)
	}
	return pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		return fn(&PostgresRepository{pool: r.pool, db: tx, inTx: true})
	})
}

const currentGeneration = "superseded_at IS NULL"

var packageCols = []string{
	"carrier", "external_ref", "weight",
	"status", "sub_status", "sub_status_desc",
	"origin_country", "origin_state", "origin_city", "origin_postal_code",
	"destination_country", "destination_state", "destination_city", "destination_postal_code",
	"service_type", "pieces", "reference_number", "last_mile_tracking_number", "last_mile_carrier_code",
	"estimated_delivery_from", "estimated_delivery_to",
	"milestones", "picked_up_at", "delivered_at",
	"latest_event_time", "latest_event_desc", "latest_event_location",
	"is_archived", "archive_key", "latest_update_at",
	"tag", "subscription_status", "source", "is_retracked", "cancelled_at",
	"register_params", "superseded_at",
	"has_return", "return_type", "return_detect_supported", "return_detected_at",
	"return_carrier_event_id", "last_callback_at",
}

func packageArgs(pkg *domain.Package) ([]any, error) {
	milestones := []byte("{}")
	if pkg.Milestones != nil {
		raw, err := json.Marshal(pkg.Milestones)
		if err != nil {
			return nil, fmt.Errorf("milestones: %w", err)
		}
		milestones = raw
	}
	params, err := json.Marshal(pkg.RegisterParams)
	if err != nil {
		return nil, fmt.Errorf("register_params: %w", err)
	}
	d := pkg.ShippingDetail
	return []any{
		pkg.Carrier, pkg.ExternalRef, pkg.Weight,
		pkg.Status, pkg.SubStatus, pkg.SubStatusDesc,
		d.Origin.Country, d.Origin.State, d.Origin.City, d.Origin.PostalCode,
		d.Destination.Country, d.Destination.State, d.Destination.City, d.Destination.PostalCode,
		d.ServiceType, d.Pieces, d.ReferenceNumber, d.LastMileTrackingNumber, d.LastMileCarrierCode,
		d.EstimatedDeliveryFrom, d.EstimatedDeliveryTo,
		milestones, pkg.PickedUpAt, pkg.DeliveredAt,
		pkg.LatestEventTime, pkg.LatestEventDesc, pkg.LatestEventLocation,
		pkg.IsArchived, pkg.ArchiveKey, pkg.LatestUpdateAt,
		pkg.Tag, pkg.SubscriptionStatus, pkg.Source, pkg.IsRetracked, pkg.CancelledAt,
		params, pkg.SupersededAt,
		pkg.HasReturn, pkg.ReturnType, pkg.ReturnDetectSupported, pkg.ReturnDetectedAt,
		pkg.ReturnCarrierEventID, pkg.LastCallbackAt,
	}, nil
}

func placeholders(from, n int) string {
	ps := make([]string, n)
	for i := range ps {
		ps[i] = fmt.Sprintf("$%d", from+i)
	}
	return strings.Join(ps, ",")
}

var packageSelect = "SELECT package_id, tracking_number, created_at, updated_at," +
	strings.Join(packageCols, ",") + " FROM tracking.package"

func (r *PostgresRepository) CreatePackage(ctx context.Context, pkg *domain.Package) error {
	if pkg.Status == "" {
		pkg.Status = domain.StatusRegistered
	}
	if pkg.SubscriptionStatus == "" {
		pkg.SubscriptionStatus = domain.SubRegistering
	}
	if pkg.Source == "" {
		pkg.Source = domain.SourceAPI
	}
	args, err := packageArgs(pkg)
	if err != nil {
		return err
	}
	row := r.db.QueryRow(ctx,
		"INSERT INTO tracking.package (tracking_number,"+strings.Join(packageCols, ",")+
			") VALUES ($1,"+placeholders(2, len(packageCols))+
			") RETURNING package_id, created_at, updated_at",
		append([]any{pkg.TrackingNumber}, args...)...,
	)
	if err := row.Scan(&pkg.PackageID, &pkg.CreatedAt, &pkg.UpdatedAt); err != nil {
		if pgerr.IsUniqueViolation(err) {
			return domain.ErrConflict
		}
		return err
	}
	return nil
}

func (r *PostgresRepository) GetPackageByID(ctx context.Context, packageID int64) (*domain.Package, error) {
	return scanPackage(r.db.QueryRow(ctx, packageSelect+" WHERE package_id=$1", packageID))
}

func (r *PostgresRepository) GetPackageByIDForUpdate(ctx context.Context, packageID int64) (*domain.Package, error) {
	return scanPackage(r.db.QueryRow(ctx, packageSelect+" WHERE package_id=$1 FOR UPDATE", packageID))
}

func (r *PostgresRepository) GetPackageByTrackingNumber(ctx context.Context, trackingNumber string) (*domain.Package, error) {
	return scanPackage(r.db.QueryRow(ctx,
		packageSelect+" WHERE tracking_number=$1 AND "+currentGeneration, trackingNumber))
}

func (r *PostgresRepository) GetPackageForUpdate(ctx context.Context, trackingNumber string) (*domain.Package, error) {
	return scanPackage(r.db.QueryRow(ctx,
		packageSelect+" WHERE tracking_number=$1 AND "+currentGeneration+" FOR UPDATE", trackingNumber))
}

func (r *PostgresRepository) UpdatePackage(ctx context.Context, pkg *domain.Package) error {
	args, err := packageArgs(pkg)
	if err != nil {
		return err
	}
	row := r.db.QueryRow(ctx,
		"UPDATE tracking.package SET ("+strings.Join(packageCols, ",")+", updated_at) = ("+
			placeholders(2, len(packageCols))+", now()) WHERE package_id=$1 RETURNING updated_at",
		append([]any{pkg.PackageID}, args...)...,
	)
	if err := row.Scan(&pkg.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ErrNotFound
		}
		return err
	}
	return nil
}

func (r *PostgresRepository) ListPackages(ctx context.Context, f domain.ListPackagesFilter) ([]domain.Package, error) {
	conds, args := packageFilterConds(f)
	limit := f.Limit
	if limit <= 0 {
		limit = 50
	}
	args = append(args, limit)
	sql := packageSelect
	if len(conds) > 0 {
		sql += " WHERE " + strings.Join(conds, " AND ")
	}
	sql += fmt.Sprintf(" ORDER BY package_id DESC LIMIT $%d", len(args))
	if f.Offset > 0 {
		args = append(args, f.Offset)
		sql += fmt.Sprintf(" OFFSET $%d", len(args))
	}
	return r.queryPackages(ctx, sql, args...)
}

func (r *PostgresRepository) ListPackageGenerations(ctx context.Context, trackingNumber string) ([]domain.Package, error) {
	return r.queryPackages(ctx, packageSelect+" WHERE tracking_number=$1 ORDER BY package_id DESC", trackingNumber)
}

func (r *PostgresRepository) queryPackages(ctx context.Context, sql string, args ...any) ([]domain.Package, error) {
	rows, err := r.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Package
	for rows.Next() {
		pkg, err := scanPackage(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *pkg)
	}
	return out, rows.Err()
}

func packageFilterConds(f domain.ListPackagesFilter) ([]string, []any) {
	var conds []string
	var args []any
	add := func(cond string, v any) {
		args = append(args, v)
		conds = append(conds, fmt.Sprintf(cond, len(args)))
	}
	if !f.IncludeSuperseded {
		conds = append(conds, currentGeneration)
	}
	if len(f.Statuses) > 0 {
		add("status = ANY($%d)", f.Statuses)
	}
	for _, c := range []struct{ col, v string }{
		{"external_ref", f.ExternalRef},
		{"origin_country", f.OriginCountry},
		{"destination_country", f.DestinationCountry},
		{"subscription_status", f.SubscriptionStatus},
		{"tag", f.Tag},
	} {
		if c.v != "" {
			add(c.col+" = $%d", c.v)
		}
	}
	if f.Source != "" && f.Source != "all" {
		add("source = $%d", f.Source)
	}
	if f.Carrier != nil {
		add("carrier = $%d", *f.Carrier)
	}
	if f.IsArchived != nil {
		add("is_archived = $%d", *f.IsArchived)
	}
	if f.HasReturn != nil {
		add("has_return = $%d", *f.HasReturn)
	}
	if f.CreatedFrom != nil {
		add("created_at >= $%d", *f.CreatedFrom)
	}
	if f.CreatedTo != nil {
		add("created_at < $%d", *f.CreatedTo)
	}
	if f.BeforeID > 0 {
		add("package_id < $%d", f.BeforeID)
	}
	return conds, args
}

func (r *PostgresRepository) ListPackagesDueArchive(ctx context.Context, inactiveBefore time.Time, limit int) ([]domain.Package, error) {
	if limit <= 0 {
		limit = 100
	}
	return r.queryPackages(ctx, packageSelect+`
 WHERE is_archived = false
   AND subscription_status NOT IN ('REGISTERING', 'TRACKING')
   AND updated_at < $1
 ORDER BY updated_at ASC
 LIMIT $2`, inactiveBefore, limit)
}

func (r *PostgresRepository) ListPackagesBySubscriptionStatus(ctx context.Context, status string, afterID int64, limit int) ([]domain.Package, error) {
	if limit <= 0 {
		limit = 200
	}
	return r.queryPackages(ctx, packageSelect+`
 WHERE subscription_status=$1 AND package_id > $2 AND `+currentGeneration+`
 ORDER BY package_id ASC
 LIMIT $3`, status, afterID, limit)
}

func (r *PostgresRepository) ListTrackingNotHeardSince(ctx context.Context, since time.Time, afterID int64, limit int) ([]domain.Package, error) {
	if limit <= 0 {
		limit = 200
	}
	return r.queryPackages(ctx, packageSelect+`
 WHERE subscription_status=$1 AND package_id > $2 AND `+currentGeneration+`
   AND (last_callback_at IS NULL OR last_callback_at < $3)
 ORDER BY package_id ASC
 LIMIT $4`, domain.SubTracking, afterID, since, limit)
}

func scanPackage(row pgx.Row) (*domain.Package, error) {
	var pkg domain.Package
	var milestones, params []byte
	d := &pkg.ShippingDetail
	err := row.Scan(
		&pkg.PackageID, &pkg.TrackingNumber, &pkg.CreatedAt, &pkg.UpdatedAt,
		&pkg.Carrier, &pkg.ExternalRef, &pkg.Weight,
		&pkg.Status, &pkg.SubStatus, &pkg.SubStatusDesc,
		&d.Origin.Country, &d.Origin.State, &d.Origin.City, &d.Origin.PostalCode,
		&d.Destination.Country, &d.Destination.State, &d.Destination.City, &d.Destination.PostalCode,
		&d.ServiceType, &d.Pieces, &d.ReferenceNumber, &d.LastMileTrackingNumber, &d.LastMileCarrierCode,
		&d.EstimatedDeliveryFrom, &d.EstimatedDeliveryTo,
		&milestones, &pkg.PickedUpAt, &pkg.DeliveredAt,
		&pkg.LatestEventTime, &pkg.LatestEventDesc, &pkg.LatestEventLocation,
		&pkg.IsArchived, &pkg.ArchiveKey, &pkg.LatestUpdateAt,
		&pkg.Tag, &pkg.SubscriptionStatus, &pkg.Source, &pkg.IsRetracked, &pkg.CancelledAt,
		&params, &pkg.SupersededAt,
		&pkg.HasReturn, &pkg.ReturnType, &pkg.ReturnDetectSupported, &pkg.ReturnDetectedAt,
		&pkg.ReturnCarrierEventID, &pkg.LastCallbackAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if len(milestones) > 0 {
		if err := json.Unmarshal(milestones, &pkg.Milestones); err != nil {
			return nil, fmt.Errorf("milestones: %w", err)
		}
	}
	if len(params) > 0 {
		if err := json.Unmarshal(params, &pkg.RegisterParams); err != nil {
			return nil, fmt.Errorf("register_params: %w", err)
		}
	}
	return &pkg, nil
}

// Insert new events; on fingerprint conflict, backfill empty time_iso/timezone from 17TRACK
// without treating the row as a newly inserted event (xmax=0 → insert).
const insertEventSQL = `
INSERT INTO tracking.tracking_event (
    package_id, event_time, time_iso, timezone, stage, sub_status, description, location,
    country, state, city, postal_code, latitude, longitude, event_fingerprint
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
ON CONFLICT (package_id, event_fingerprint) DO UPDATE
SET
  time_iso = CASE
    WHEN EXCLUDED.time_iso <> '' THEN EXCLUDED.time_iso
    ELSE tracking.tracking_event.time_iso
  END,
  timezone = CASE
    WHEN EXCLUDED.timezone <> '' THEN EXCLUDED.timezone
    ELSE tracking.tracking_event.timezone
  END
WHERE (tracking.tracking_event.time_iso = '' AND EXCLUDED.time_iso <> '')
   OR (tracking.tracking_event.timezone = '' AND EXCLUDED.timezone <> '')
RETURNING event_id, created_at, (xmax = 0) AS is_insert`

func (r *PostgresRepository) InsertEvents(ctx context.Context, events []domain.TrackingEvent) ([]domain.TrackingEvent, error) {
	if len(events) == 0 {
		return nil, nil
	}
	batch := &pgx.Batch{}
	for _, ev := range events {
		batch.Queue(insertEventSQL,
			ev.PackageID, ev.EventTime, ev.TimeISO, ev.Timezone,
			ev.Stage, ev.SubStatus, ev.Description, ev.Location,
			ev.Address.Country, ev.Address.State, ev.Address.City, ev.Address.PostalCode,
			ev.Latitude, ev.Longitude, ev.EventFingerprint,
		)
	}
	br := r.db.SendBatch(ctx, batch)
	defer func() { _ = br.Close() }()
	var inserted []domain.TrackingEvent
	for _, ev := range events {
		var isInsert bool
		err := br.QueryRow().Scan(&ev.EventID, &ev.CreatedAt, &isInsert)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if isInsert {
			inserted = append(inserted, ev)
		}
	}
	return inserted, nil
}

func (r *PostgresRepository) ListEventsByPackage(ctx context.Context, packageID int64) ([]domain.TrackingEvent, error) {
	rows, err := r.db.Query(ctx, `
SELECT e.event_id, e.package_id, p.tracking_number, e.event_time, e.time_iso, e.timezone,
       e.stage, e.sub_status,
       e.description, e.location, e.country, e.state, e.city, e.postal_code,
       e.latitude, e.longitude, e.event_fingerprint, e.created_at
FROM tracking.tracking_event e
JOIN tracking.package p ON p.package_id = e.package_id
WHERE e.package_id=$1
ORDER BY e.event_time ASC, e.event_id ASC`, packageID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.TrackingEvent
	for rows.Next() {
		var ev domain.TrackingEvent
		if err := rows.Scan(
			&ev.EventID, &ev.PackageID, &ev.TrackingNumber, &ev.EventTime,
			&ev.TimeISO, &ev.Timezone,
			&ev.Stage, &ev.SubStatus,
			&ev.Description, &ev.Location,
			&ev.Address.Country, &ev.Address.State, &ev.Address.City, &ev.Address.PostalCode,
			&ev.Latitude, &ev.Longitude, &ev.EventFingerprint, &ev.CreatedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}

func (r *PostgresRepository) DeleteEventsByPackage(ctx context.Context, packageID int64) error {
	_, err := r.db.Exec(ctx, `DELETE FROM tracking.tracking_event WHERE package_id=$1`, packageID)
	return err
}

func (r *PostgresRepository) TryLock(ctx context.Context, name string) (func(), bool, error) {
	conn, err := r.pool.Acquire(ctx)
	if err != nil {
		return nil, false, err
	}
	var ok bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtext($1))`, name).Scan(&ok); err != nil {
		conn.Release()
		return nil, false, err
	}
	if !ok {
		conn.Release()
		return nil, false, nil
	}
	release := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := conn.Exec(ctx, `SELECT pg_advisory_unlock(hashtext($1))`, name); err != nil {
			_ = conn.Conn().Close(ctx)
		}
		conn.Release()
	}
	return release, true, nil
}
