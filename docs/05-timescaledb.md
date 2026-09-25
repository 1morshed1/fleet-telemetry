# 05 — TimescaleDB & PostGIS storage

## What you'll learn
- What a **hypertable** is and why time-partitioning keeps queries fast as data grows.
- **Continuous aggregates**: precomputing summaries so dashboards are instant.
- **Compression** and **retention** policies — controlling disk without losing recent detail.
- **PostGIS** basics: storing points/polygons and how we insert geometry efficiently.

## The problem

GPS events pour in forever. A normal Postgres table would grow to billions of rows, and every
"last hour for vehicle X" query would get slower as the table got bigger. We also need
**spatial** types (points, polygons) and spatial queries. Two Postgres extensions solve this:
**TimescaleDB** (time-series) and **PostGIS** (geospatial).

Schema: [`db/init/01-schema.sql`](../db/init/01-schema.sql). Seed zones:
[`db/init/02-seed-geofences.sql`](../db/init/02-seed-geofences.sql).

## Hypertables: automatic time-partitioning

```sql
CREATE TABLE gps_events ( time TIMESTAMPTZ NOT NULL, vehicle_id TEXT, location GEOMETRY(Point,4326), ... );
SELECT create_hypertable('gps_events', 'time');
```

`create_hypertable` turns one logical table into many physical **chunks**, each holding a time
range (default 7 days). To you it's still one table — `SELECT`/`INSERT` are unchanged — but:

- A query with a time filter (`WHERE time > now() - interval '1 hour'`) only scans the chunk(s)
  covering that range. This is **chunk exclusion**, and it's why queries stay fast at any table
  size.
- Old chunks can be compressed or dropped as whole units — cheap, no giant `DELETE`.

Our index `(vehicle_id, time DESC)` then makes "latest position per vehicle" and "vehicle X's
recent history" fast *within* the relevant chunks.

## Continuous aggregates: precompute the dashboard's questions

Dashboards ask the same aggregate questions constantly ("avg/max speed per vehicle per hour").
Recomputing that from raw rows every time is wasteful. A **continuous aggregate** is a
materialized view that TimescaleDB keeps **incrementally up to date**:

```sql
CREATE MATERIALIZED VIEW hourly_vehicle_summary
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 hour', time) AS bucket, vehicle_id,
       AVG(speed_kmh) AS avg_speed, MAX(speed_kmh) AS max_speed, COUNT(*) AS ping_count
FROM gps_events GROUP BY bucket, vehicle_id;

SELECT add_continuous_aggregate_policy('hourly_vehicle_summary',
    start_offset => INTERVAL '3 hours', end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '30 minutes');
```

`time_bucket` is Timescale's `GROUP BY`-friendly time flooring. The policy refreshes only the
recently-changed buckets, so the summary is cheap to maintain and instant to read.

### A gotcha we hit

The plan's aggregate included `ST_MakeLine(location ORDER BY time)` to precompute each route
as a line. **Continuous aggregates don't allow ordered-set aggregates** (anything with
`ORDER BY` inside the aggregate). So we dropped route geometry from the aggregate and build it
**on demand** in the dashboard API instead (`ST_MakeLine` over a bounded time range is cheap
because of chunk exclusion). Documented in the schema comment and mentioned here because it's
exactly the kind of surprise you learn from.

## Compression & retention

```sql
ALTER TABLE gps_events SET (timescaledb.compress,
    timescaledb.compress_segmentby = 'vehicle_id',
    timescaledb.compress_orderby   = 'time DESC');
SELECT add_compression_policy('gps_events', INTERVAL '7 days');   -- compress chunks >7d old
SELECT add_retention_policy ('gps_events', INTERVAL '90 days');   -- drop chunks >90d old
```

- **Compression** converts old row-based chunks into a columnar form. Time-series data
  compresses extremely well (Timescale cites 90%+) because neighboring values are similar.
  `segmentby = vehicle_id` keeps per-vehicle scans efficient even when compressed.
- **Retention** drops whole chunks past 90 days — instant, unlike a row-by-row `DELETE`.

The mental model: **hot recent data** stays uncompressed and fast to write/scan; **warm older
data** gets compressed; **cold ancient data** is dropped. You tune the thresholds to your
storage budget.

## PostGIS: geometry the right way

- `GEOMETRY(Point, 4326)` / `GEOMETRY(Polygon, 4326)` — SRID **4326** is WGS84, ordinary GPS
  lat/lng.
- Polygons in WKT use **"longitude latitude"** order and must be **closed** (first point =
  last point) — see the seed file.
- A **GIST** index on the polygon column makes containment/intersection queries fast (our
  correctness cross-check for the H3 geofencing).

### Inserting geometry efficiently

You can't build geometry with plain values — you need `ST_MakePoint(lng, lat)`. But we still
want **batch** inserts, not one round-trip per row. The trick is `unnest` of parallel arrays
([`internal/store/store.go`](../internal/store/store.go)):

```sql
INSERT INTO gps_events (time, vehicle_id, location, speed_kmh, heading, geofence_status)
SELECT t, v, ST_SetSRID(ST_MakePoint(lng, lat), 4326), s, h, st
FROM unnest($1::timestamptz[], $2::text[], $3::float8[], $4::float8[],
            $5::float8[], $6::float8[], $7::text[]) AS u(t, v, lng, lat, s, h, st);
```

One statement inserts the whole batch, and `ST_MakePoint` runs per row inside the database.
(pgx's `CopyFrom` would be even faster but can't call `ST_MakePoint`; `unnest` is the
pragmatic middle ground.)

Reading geometry back out for the API uses `ST_AsGeoJSON`, so the browser gets standard GeoJSON
it can drop straight onto a Leaflet map ([06-dashboard.md](06-dashboard.md)).

## Alerts tables

`speed_alerts` and `geofence_alerts` are also hypertables (they're time-series too). The
processor writes to them on transitions; the dashboard reads recent ones for the alerts feed.
