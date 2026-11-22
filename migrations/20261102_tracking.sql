-- Package-centric tracking store (schema: tracking).
-- Owned by this service: embedded and applied on cmd/tracking startup
-- (ledger: tracking.schema_migrations). Same model as GoTrue for auth.
-- Hot: package + tracking_event; cold archive on S3-compatible storage (archive_key).
-- Unit of tracking: a package generation. Carriers recycle numbers, so
-- tracking_number is unique only among current generations (superseded_at IS NULL).
-- TrackingService is the single 17TRACK subscriber; shipment waybills use source='shipment'.
-- STOPPED = provider stop, retrackable once.
--
-- Logistics columns mirror 17TRACK TrackDetail / track_info; service columns
-- (subscription, archive, return) are service-owned.

CREATE SCHEMA IF NOT EXISTS tracking;

CREATE TABLE IF NOT EXISTS tracking.package (
    package_id              bigserial PRIMARY KEY,
    tracking_number         text NOT NULL,           -- 17TRACK number
    external_ref            text NOT NULL DEFAULT '', -- shipment / order correlation
    weight                  numeric,                  -- misc_info.weight_kg

    -- 17TRACK enums: domain.Status* / SubStatus* (see internal/domain/track17_const.go).
    -- status='registered' is local-only until the first 17TRACK sync.
    status                  text NOT NULL DEFAULT 'registered', -- latest_status.status
    sub_status              text NOT NULL DEFAULT '',           -- latest_status.sub_status
    sub_status_desc         text NOT NULL DEFAULT '',           -- latest_status.sub_status_descr

    -- shipping_info.shipper_address / recipient_address (+ register extras)
    origin_country          text NOT NULL DEFAULT '',
    origin_state            text NOT NULL DEFAULT '',
    origin_city             text NOT NULL DEFAULT '',
    origin_postal_code      text NOT NULL DEFAULT '',
    destination_country     text NOT NULL DEFAULT '',
    destination_state       text NOT NULL DEFAULT '',
    destination_city        text NOT NULL DEFAULT '',
    destination_postal_code text NOT NULL DEFAULT '',
    service_type            text NOT NULL DEFAULT '', -- misc_info.service_type
    pieces                  int,                      -- misc_info.pieces
    reference_number        text NOT NULL DEFAULT '', -- misc_info.reference_number
    last_mile_tracking_number text NOT NULL DEFAULT '', -- misc_info.local_number
    last_mile_carrier_code  text NOT NULL DEFAULT '', -- misc_info.local_provider
    estimated_delivery_from timestamptz, -- time_metrics.estimated_delivery_date.from
    estimated_delivery_to   timestamptz, -- time_metrics.estimated_delivery_date.to

    milestones              jsonb NOT NULL DEFAULT '{}', -- track_info.milestone
    picked_up_at            timestamptz,
    delivered_at            timestamptz,

    latest_event_time       timestamptz,
    latest_event_desc       text NOT NULL DEFAULT '',
    latest_event_location   text NOT NULL DEFAULT '',

    is_archived             boolean NOT NULL DEFAULT false,
    archive_key             text NOT NULL DEFAULT '',
    latest_update_at        timestamptz,

    -- 17TRACK subscription control
    carrier                 int NOT NULL DEFAULT 0, -- 17TRACK carrier key (0 = auto-detect)
    tag                     text, -- 17TRACK tag
    subscription_status     text NOT NULL DEFAULT 'REGISTERING',
    source                  text NOT NULL DEFAULT 'api',
    is_retracked            boolean NOT NULL DEFAULT false,
    cancelled_at            timestamptz,
    register_params         jsonb NOT NULL DEFAULT '{}', -- last /register request for retries
    superseded_at           timestamptz, -- set when a newer generation of the same number was registered

    -- Return detection (service rules on 17TRACK events)
    has_return              boolean NOT NULL DEFAULT false,
    return_type             text,
    return_detect_supported boolean NOT NULL DEFAULT true,
    return_detected_at      timestamptz,
    return_carrier_event_id text,
    last_callback_at        timestamptz, -- last 17TRACK data received (webhook or poll)

    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now()
);

-- At most one current generation per tracking_number; history stays by package_id.
CREATE UNIQUE INDEX IF NOT EXISTS package_current_number_key
    ON tracking.package (tracking_number) WHERE superseded_at IS NULL;
CREATE INDEX IF NOT EXISTS package_number_history_idx
    ON tracking.package (tracking_number, package_id DESC);
CREATE INDEX IF NOT EXISTS package_status_idx
    ON tracking.package (status, package_id DESC);
CREATE INDEX IF NOT EXISTS package_subscription_id_idx
    ON tracking.package (subscription_status, package_id);
-- Archive: inactive packages untouched for the retention window (updated_at anchor).
CREATE INDEX IF NOT EXISTS package_due_archive_idx
    ON tracking.package (updated_at)
    WHERE is_archived = false AND subscription_status NOT IN ('REGISTERING', 'TRACKING');
CREATE INDEX IF NOT EXISTS package_return_idx
    ON tracking.package (has_return, status);

COMMENT ON TABLE tracking.package IS
    '包裹聚合：17TRACK 物流快照 + 订阅/归档/退件；运单号可被承运商回收，每行是一代，当前代 superseded_at IS NULL 唯一。';
COMMENT ON COLUMN tracking.package.carrier IS '17TRACK carrier key (0 = auto-detect)';
COMMENT ON COLUMN tracking.package.superseded_at IS 'set when a newer generation of the same tracking_number was registered';
COMMENT ON COLUMN tracking.package.last_callback_at IS
    'last 17TRACK data received (webhook or poll); SyncActive skips recently heard packages';

CREATE TABLE IF NOT EXISTS tracking.tracking_event (
    event_id            bigserial PRIMARY KEY,
    package_id          bigint NOT NULL REFERENCES tracking.package (package_id) ON DELETE CASCADE,
    event_time          timestamptz NOT NULL,
    stage               text NOT NULL DEFAULT '', -- event.stage / milestone.key_stage (domain.Stage*)
    sub_status          text NOT NULL DEFAULT '', -- event.sub_status (domain.SubStatus*)
    description         text NOT NULL DEFAULT '',
    location            text NOT NULL DEFAULT '',
    country             text NOT NULL DEFAULT '', -- event.address
    state               text NOT NULL DEFAULT '',
    city                text NOT NULL DEFAULT '',
    postal_code         text NOT NULL DEFAULT '',
    latitude            double precision, -- event.address.coordinates
    longitude           double precision,
    event_fingerprint   text NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT now(),
    UNIQUE (package_id, event_fingerprint)
);

CREATE INDEX IF NOT EXISTS tracking_event_package_time_idx
    ON tracking.tracking_event (package_id, event_time ASC, event_id ASC);

COMMENT ON TABLE tracking.tracking_event IS
    '轨迹事件热存（对齐 17TRACK providers[].events）；按指纹去重；归档后删热行。';
