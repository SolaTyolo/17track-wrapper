package track17

import (
	"encoding/json"
	"testing"
	"time"
)

func TestShouldStopTracking(t *testing.T) {
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	old := now.Add(-8 * 24 * time.Hour)
	if !ShouldStopTracking(now, "Delivered", &old, &old) {
		t.Fatal("delivered 8d ago should stop")
	}
	fresh := now.Add(-2 * 24 * time.Hour)
	if ShouldStopTracking(now, "Delivered", &fresh, &fresh) {
		t.Fatal("delivered 2d ago should keep tracking")
	}
	stale := now.Add(-31 * 24 * time.Hour)
	if !ShouldStopTracking(now, "InTransit", &stale, nil) {
		t.Fatal("31d no event should stop")
	}
}

func TestParseDetailJSON(t *testing.T) {
	raw := json.RawMessage(`{
	  "number": "1Z",
	  "carrier": 100002,
	  "track_info": {
	    "shipping_info": {
	      "shipper_address": {"country": "CN", "state": "GD", "city": "SZ", "postal_code": "518000"},
	      "recipient_address": {"country": "US", "state": "CA", "city": "LA", "postal_code": "90001"}
	    },
	    "latest_status": {
	      "status": "Delivered",
	      "sub_status": "Delivered_Other",
	      "sub_status_descr": "Delivered"
	    },
	    "latest_event": {
	      "time_utc": "2026-10-01T17:00:00Z",
	      "time_iso": "2026-10-01T12:00:00-05:00",
	      "time_raw": {"timezone": "-05:00"},
	      "sub_status": "Delivered_Other"
	    },
	    "time_metrics": {
	      "estimated_delivery_date": {"from": "2026-09-28T00:00:00Z", "to": "2026-09-30T00:00:00Z"}
	    },
	    "misc_info": {
	      "service_type": "Ground",
	      "weight_kg": "1.25",
	      "pieces": "2",
	      "reference_number": "PO-1",
	      "local_number": "1ZLAST",
	      "local_provider": "UPS"
	    },
	    "milestone": [
	      {"key_stage": "PickedUp", "time_utc": "2026-09-20T10:00:00Z"},
	      {"key_stage": "Delivered", "time_utc": "2026-10-01T17:00:00Z"}
	    ],
	    "tracking": {
	      "providers": [{
	        "provider": {"homepage": "https://www.ups.com"},
	        "events": [{
	          "time_iso": "2026-10-01T12:00:00-05:00",
	          "time_utc": "2026-10-01T17:00:00Z",
	          "time_raw": {"timezone": "-05:00"},
	          "description": "Delivered",
	          "location": "NY",
	          "stage": "Delivered",
	          "sub_status": "Delivered_Other",
	          "address": {"country": "US", "state": "NY", "city": "New York", "postal_code": "10001"}
	        }]
	      }]
	    }
	  }
	}`)
	p, err := ParseDetailJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	if p.Status != "Delivered" || p.SubStatus != "Delivered_Other" || p.SubStatusDesc != "Delivered" {
		t.Fatalf("status %s %s %s", p.Status, p.SubStatus, p.SubStatusDesc)
	}
	if p.Number != "1Z" || p.Carrier != 100002 {
		t.Fatalf("number/carrier = %q %d", p.Number, p.Carrier)
	}
	if p.OfficialURL != "https://www.ups.com" || p.Timezone != "-05:00" || len(p.Events) != 1 {
		t.Fatalf("%#v", p)
	}
	if p.LastEventAt != "2026-10-01T17:00:00Z" {
		t.Fatalf("LastEventAt must be time_utc only, got %q", p.LastEventAt)
	}
	if p.OriginCountry != "CN" || p.DestinationCity != "LA" || p.DestinationPostal != "90001" {
		t.Fatalf("shipping %#v", p)
	}
	if p.ServiceType != "Ground" || p.LocalNumber != "1ZLAST" || p.LocalProvider != "UPS" {
		t.Fatalf("misc %#v", p)
	}
	if p.WeightKG == nil || *p.WeightKG != 1.25 || p.Pieces == nil || *p.Pieces != 2 {
		t.Fatalf("weight/pieces %#v", p)
	}
	if p.Milestones["PickedUp"] == "" || p.DeliveredAt == "" || p.PickupAt == "" {
		t.Fatalf("milestones %#v pickup=%s delivered=%s", p.Milestones, p.PickupAt, p.DeliveredAt)
	}
	if p.Events[0].Country != "US" || p.Events[0].PostalCode != "10001" {
		t.Fatalf("event address %#v", p.Events[0])
	}
}

func TestTokenFromEnv(t *testing.T) {
	t.Setenv(EnvSecret, "")
	if TokenFromEnv() != "" {
		t.Fatal("expected empty")
	}
	t.Setenv(EnvSecret, "sek")
	if TokenFromEnv() != "sek" {
		t.Fatalf("got %q", TokenFromEnv())
	}
}
