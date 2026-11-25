package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

// TrackingEvent is one logistics scan / status line for a package
// (aligned with 17TRACK providers[].events).
type TrackingEvent struct {
	EventID          int64     `json:"event_id,omitempty"`
	PackageID        int64     `json:"package_id"`
	TrackingNumber   string    `json:"tracking_number"`
	EventTime        time.Time `json:"event_time"`
	// TimeISO / Timezone preserve 17TRACK local wall clock for UI (not browser TZ).
	TimeISO          string    `json:"time_iso,omitempty"`
	Timezone         string    `json:"timezone,omitempty"`
	Stage            string    `json:"stage,omitempty"`
	SubStatus        string    `json:"sub_status,omitempty"`
	Description      string    `json:"description,omitempty"`
	Location         string    `json:"location,omitempty"`
	Address          Address   `json:"address"`
	Latitude         *float64  `json:"lat,omitempty"`
	Longitude        *float64  `json:"lng,omitempty"`
	EventFingerprint string    `json:"event_fingerprint"`
	CreatedAt        time.Time `json:"created_at,omitempty"`
}

// Fingerprint builds a stable idempotency key for an event line.
func Fingerprint(trackingNumber string, eventTime time.Time, location, description, stage string) string {
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "%s|%d|%s|%s|%s",
		trackingNumber, eventTime.UTC().UnixNano(), location, description, stage)
	return hex.EncodeToString(h.Sum(nil))
}

// EventBatch is one normalized tracking update for one tracking number.
type EventBatch struct {
	TrackingNumber string
	UpdateTime     time.Time
	Status         string
	SubStatus      string
	SubStatusDesc  string
	Weight         *float64
	ShippingDetail ShippingDetail
	Milestones     Milestones
	Events         []TrackingEvent
}

// ArchiveSnapshot is the cold JSON document stored for an archived package.
type ArchiveSnapshot struct {
	Package    Package         `json:"package"`
	Events     []TrackingEvent `json:"events"`
	ArchivedAt time.Time       `json:"archived_at"`
}
