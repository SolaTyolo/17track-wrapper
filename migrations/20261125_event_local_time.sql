-- Backfill for DBs that already applied 20261102 (CREATE TABLE lacked local-time cols).
-- Fresh installs get these columns from 20261102_tracking.sql; ADD IF NOT EXISTS is a no-op.
ALTER TABLE tracking.tracking_event
  ADD COLUMN IF NOT EXISTS time_iso text NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS timezone text NOT NULL DEFAULT '';

COMMENT ON COLUMN tracking.tracking_event.event_time IS
    '事件绝对时间（timestamptz / UTC）；排序与指纹用；优先取 17TRACK time_utc。';
COMMENT ON COLUMN tracking.tracking_event.time_iso IS
    '17TRACK event.time_iso：当地墙钟+offset（如 2026-11-24T15:07:00+01:00）；UI 展示用，勿按浏览器时区再转。';
COMMENT ON COLUMN tracking.tracking_event.timezone IS
    '17TRACK event.time_raw.timezone：当地偏移（如 +01:00）；time_iso 缺失时可用 event_time+offset 还原墙钟。';
