-- Fleet Telemetry schema. Runs once on first container start (Postgres image
-- executes files in /docker-entrypoint-initdb.d in name order).
--
-- Two extensions do the heavy lifting:
--   timescaledb  -> hypertables (auto time-partitioning), continuous aggregates,
--                   compression, retention.
--   postgis      -> geometry column types + spatial indexes/queries.

CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS postgis;

-- ---------------------------------------------------------------------------
-- gps_events: one row per processed ping. This is the high-volume table.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gps_events (
    time            TIMESTAMPTZ        NOT NULL,
    vehicle_id      TEXT               NOT NULL,
    location        GEOMETRY(Point, 4326) NOT NULL,  -- 4326 = WGS84 lat/lng
    speed_kmh       DOUBLE PRECISION,                -- calculated by the processor
    heading         DOUBLE PRECISION,
    geofence_status TEXT DEFAULT 'clear'             -- 'clear' | 'violation'
);

-- Turn it into a hypertable: TimescaleDB transparently partitions rows into
-- time-based "chunks" (default 7 days). Queries with a time filter only touch
-- the relevant chunks instead of scanning everything.
SELECT create_hypertable('gps_events', 'time', if_not_exists => TRUE);

-- Index for "latest position per vehicle" and per-vehicle history scans.
CREATE INDEX IF NOT EXISTS idx_gps_events_vehicle_time
    ON gps_events (vehicle_id, time DESC);

-- ---------------------------------------------------------------------------
-- geofence_zones: polygons the processor watches. Small, read-mostly.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS geofence_zones (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    zone_type   TEXT NOT NULL,               -- 'restricted' | 'warehouse' | 'delivery_area'
    boundary    GEOMETRY(Polygon, 4326) NOT NULL,
    speed_limit DOUBLE PRECISION             -- km/h; NULL = no explicit limit
);

-- GIST spatial index makes PostGIS containment checks fast (used as a
-- correctness cross-check against the processor's H3 index).
CREATE INDEX IF NOT EXISTS idx_geofence_boundary
    ON geofence_zones USING GIST (boundary);

-- ---------------------------------------------------------------------------
-- Alerts. The processor writes here when it detects a violation.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS speed_alerts (
    time        TIMESTAMPTZ NOT NULL,
    vehicle_id  TEXT NOT NULL,
    speed_kmh   DOUBLE PRECISION NOT NULL,
    limit_kmh   DOUBLE PRECISION NOT NULL,
    location    GEOMETRY(Point, 4326) NOT NULL
);
SELECT create_hypertable('speed_alerts', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_speed_alerts_vehicle_time
    ON speed_alerts (vehicle_id, time DESC);

CREATE TABLE IF NOT EXISTS geofence_alerts (
    time        TIMESTAMPTZ NOT NULL,
    vehicle_id  TEXT NOT NULL,
    zone_id     INTEGER NOT NULL,
    zone_name   TEXT NOT NULL,
    location    GEOMETRY(Point, 4326) NOT NULL
);
SELECT create_hypertable('geofence_alerts', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_geofence_alerts_time
    ON geofence_alerts (time DESC);

-- ---------------------------------------------------------------------------
-- Continuous aggregate: precomputed hourly per-vehicle stats for the dashboard.
--
-- NOTE: TimescaleDB continuous aggregates do NOT allow ordered-set aggregates
-- (e.g. ST_MakeLine(... ORDER BY time)). We therefore keep only scalar stats
-- here and build route geometry on demand in the API. See docs/05-timescaledb.md.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_vehicle_summary
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    vehicle_id,
    AVG(speed_kmh)              AS avg_speed,
    MAX(speed_kmh)             AS max_speed,
    COUNT(*)                    AS ping_count
FROM gps_events
GROUP BY bucket, vehicle_id
WITH NO DATA;

-- Refresh policy: keep the aggregate current automatically.
SELECT add_continuous_aggregate_policy('hourly_vehicle_summary',
    start_offset      => INTERVAL '3 hours',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '30 minutes',
    if_not_exists     => TRUE);

-- Compression: after 7 days, compress old chunks (Timescale claims 90%+ on
-- time-series). Segment by vehicle_id so per-vehicle scans stay efficient.
ALTER TABLE gps_events SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'vehicle_id',
    timescaledb.compress_orderby   = 'time DESC'
);
SELECT add_compression_policy('gps_events', INTERVAL '7 days', if_not_exists => TRUE);

-- Retention: drop data older than 90 days so the disk doesn't grow forever.
SELECT add_retention_policy('gps_events', INTERVAL '90 days', if_not_exists => TRUE);
