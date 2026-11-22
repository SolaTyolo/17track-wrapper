// Package track17 holds service-specific helpers around github.com/SolaTyolo/17track-sdk.
// Call the SDK client directly for Register / GetTrackInfo / webhook verify — no HTTP proxy here.
package track17

import (
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"time"

	sdk "github.com/SolaTyolo/17track-sdk"
)

const (
	TrackingStatusTracking = "Tracking"

	// StagePickedUp matches event.stage / milestone.key_stage (domain.StagePickedUp).
	StagePickedUp = "PickedUp"

	EnvSecret  = "TRACK17_SECRET"
	EnvBaseURL = "TRACK17_BASE_URL"
)

// TokenFromEnv reads TRACK17_SECRET.
func TokenFromEnv() string {
	return strings.TrimSpace(os.Getenv(EnvSecret))
}

// ClientOptionsFromEnv returns SDK options from env (e.g. TRACK17_BASE_URL for mocks).
func ClientOptionsFromEnv() []sdk.Option {
	if base := strings.TrimSpace(os.Getenv(EnvBaseURL)); base != "" {
		return []sdk.Option{sdk.WithBaseURL(base)}
	}
	return nil
}

type Event struct {
	TimeISO     string
	TimeUTC     string
	Timezone    string
	Description string
	Location    string
	Stage       string
	SubStatus   string
	Country     string
	State       string
	City        string
	PostalCode  string
	Lat         *float64 `json:",omitempty"`
	Lng         *float64 `json:",omitempty"`
	Raw         json.RawMessage
}

type ParsedTrack struct {
	Number         string
	Carrier        int // 17TRACK carrier key from webhook / gettrackinfo
	Status         string
	SubStatus      string
	SubStatusDesc  string
	TrackingStatus string
	PickupAt       string
	DeliveredAt    string
	LastEventAt    string
	LastPushAt     string
	Timezone       string
	OfficialURL    string
	Events         []Event

	// Shipping / misc mirrored from track_info (17TRACK field names → local columns).
	OriginCountry         string
	OriginState           string
	OriginCity            string
	OriginPostalCode      string
	DestinationCountry    string
	DestinationState      string
	DestinationCity       string
	DestinationPostal     string
	ServiceType           string
	WeightKG              *float64
	Pieces                *int
	ReferenceNumber       string
	LocalNumber           string // → last_mile_tracking_number
	LocalProvider         string // → last_mile_carrier_code
	EstimatedDeliveryFrom string
	EstimatedDeliveryTo   string
	// Milestones maps key_stage → RFC3339 UTC time.
	Milestones map[string]string
}

// ParseDetail maps an SDK TrackDetail (gettrackinfo / webhook data) into service fields.
func ParseDetail(detail *sdk.TrackDetail) ParsedTrack {
	p := ParsedTrack{Milestones: map[string]string{}}
	if detail == nil {
		return p
	}
	p.Number = strings.TrimSpace(detail.Number)
	p.Carrier = detail.Carrier
	// Flat registration extras on TrackDetail (when present).
	p.OriginCountry = strings.TrimSpace(detail.OriginCountry)
	p.DestinationCountry = strings.TrimSpace(detail.DestinationCountry)
	p.DestinationCity = strings.TrimSpace(detail.DestinationCity)
	p.DestinationPostal = strings.TrimSpace(detail.DestinationPostal)

	ti := detail.TrackInfo
	if ti == nil {
		return p
	}
	if ti.LatestStatus != nil {
		p.Status = ti.LatestStatus.Status
		p.SubStatus = ti.LatestStatus.SubStatus
		p.SubStatusDesc = ti.LatestStatus.SubStatusDescr
	}
	if ti.LatestEvent != nil {
		// Prefer time_utc only — time_iso is local-with-offset and must not drive
		// latest_event_time / UpdateTime (stored as timestamptz UTC).
		p.LastEventAt = strings.TrimSpace(ti.LatestEvent.TimeUTC)
		if ti.LatestEvent.TimeRaw != nil {
			p.Timezone = ti.LatestEvent.TimeRaw.Timezone
		}
	}
	if t, ok := sdk.DeliveryTimeUTC(detail); ok {
		p.DeliveredAt = t.UTC().Format(time.RFC3339)
	}

	if si := ti.ShippingInfo; si != nil {
		if a := si.ShipperAddress; a != nil {
			p.OriginCountry = firstNonEmpty(strings.TrimSpace(a.Country), p.OriginCountry)
			p.OriginState = strings.TrimSpace(a.State)
			p.OriginCity = strings.TrimSpace(a.City)
			p.OriginPostalCode = strings.TrimSpace(a.PostalCode)
		}
		if a := si.RecipientAddress; a != nil {
			p.DestinationCountry = firstNonEmpty(strings.TrimSpace(a.Country), p.DestinationCountry)
			p.DestinationState = strings.TrimSpace(a.State)
			p.DestinationCity = firstNonEmpty(strings.TrimSpace(a.City), p.DestinationCity)
			p.DestinationPostal = firstNonEmpty(strings.TrimSpace(a.PostalCode), p.DestinationPostal)
		}
	}

	if mi := ti.MiscInfo; mi != nil {
		p.ServiceType = strings.TrimSpace(mi.ServiceType)
		p.ReferenceNumber = strings.TrimSpace(mi.ReferenceNumber)
		p.LocalNumber = strings.TrimSpace(mi.LocalNumber)
		p.LocalProvider = strings.TrimSpace(mi.LocalProvider)
		if w := strings.TrimSpace(mi.WeightKG); w != "" {
			if f, err := strconv.ParseFloat(w, 64); err == nil {
				p.WeightKG = &f
			}
		}
		if pc := strings.TrimSpace(mi.Pieces); pc != "" {
			if n, err := strconv.Atoi(pc); err == nil {
				p.Pieces = &n
			}
		}
	}

	if tm := ti.TimeMetrics; tm != nil && tm.EstimatedDeliveryDate != nil {
		p.EstimatedDeliveryFrom = strings.TrimSpace(tm.EstimatedDeliveryDate.From)
		p.EstimatedDeliveryTo = strings.TrimSpace(tm.EstimatedDeliveryDate.To)
	}

	for _, m := range ti.Milestone {
		key := strings.TrimSpace(m.KeyStage)
		if key == "" {
			continue
		}
		ts := firstNonEmpty(m.TimeUTC, m.TimeISO)
		if ts == "" {
			continue
		}
		p.Milestones[key] = ts
		if p.PickupAt == "" && (key == "PickedUp" || strings.EqualFold(key, "PickedUp")) {
			p.PickupAt = ts
		}
		if p.DeliveredAt == "" && (key == "Delivered" || strings.EqualFold(key, "Delivered")) {
			p.DeliveredAt = ts
		}
	}

	if ti.Tracking != nil {
		for i, prov := range ti.Tracking.Providers {
			if i == 0 && prov.Provider != nil {
				p.OfficialURL = prov.Provider.Homepage
			}
			if p.ServiceType == "" {
				p.ServiceType = strings.TrimSpace(prov.ServiceType)
			}
			for _, ev := range prov.Events {
				tz := ""
				if ev.TimeRaw != nil {
					tz = ev.TimeRaw.Timezone
				}
				if p.Timezone == "" && tz != "" {
					p.Timezone = tz
				}
				rawEv, _ := json.Marshal(ev)
				e := Event{
					TimeISO:     ev.TimeISO,
					TimeUTC:     ev.TimeUTC,
					Timezone:    tz,
					Description: ev.Description,
					Location:    ev.Location,
					Stage:       ev.Stage,
					SubStatus:   ev.SubStatus,
					Raw:         rawEv,
				}
				if ev.Address != nil {
					e.Country = strings.TrimSpace(ev.Address.Country)
					e.State = strings.TrimSpace(ev.Address.State)
					e.City = strings.TrimSpace(ev.Address.City)
					e.PostalCode = strings.TrimSpace(ev.Address.PostalCode)
					if c := ev.Address.Coordinates; c != nil {
						e.Lat, e.Lng = parseCoord(c.Latitude, 90), parseCoord(c.Longitude, 180)
						if e.Lat == nil || e.Lng == nil {
							e.Lat, e.Lng = nil, nil
						}
					}
				}
				p.Events = append(p.Events, e)
				if p.PickupAt == "" && (ev.SubStatus == sdk.SubStatusInTransitPickedUp || ev.Stage == StagePickedUp) {
					p.PickupAt = firstNonEmpty(ev.TimeUTC, ev.TimeISO)
				}
			}
		}
	}
	if p.TrackingStatus == "" {
		p.TrackingStatus = TrackingStatusTracking
	}
	return p
}

// ParseDetailJSON unmarshals webhook / ApplyWebhook data into ParsedTrack.
func ParseDetailJSON(raw json.RawMessage) (ParsedTrack, error) {
	if len(raw) == 0 {
		return ParsedTrack{}, nil
	}
	var detail sdk.TrackDetail
	if err := json.Unmarshal(raw, &detail); err != nil {
		return ParsedTrack{}, err
	}
	return ParseDetail(&detail), nil
}

// parseCoord parses a 17TRACK string coordinate; empty, malformed or out-of-range → nil.
func parseCoord(s string, limit float64) *float64 {
	f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil || f < -limit || f > limit {
		return nil
	}
	return &f
}

func ShouldStopTracking(now time.Time, status string, lastEvent *time.Time, delivered *time.Time) bool {
	if lastEvent == nil {
		return false
	}
	if status == "Delivered" {
		cutoff := now.Add(-7 * 24 * time.Hour)
		if delivered != nil {
			return delivered.Before(cutoff) && lastEvent.Before(cutoff)
		}
		return lastEvent.Before(cutoff)
	}
	return lastEvent.Before(now.Add(-30 * 24 * time.Hour))
}

func firstNonEmpty(vs ...string) string {
	for _, v := range vs {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
