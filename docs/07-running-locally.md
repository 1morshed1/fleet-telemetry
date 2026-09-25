# 07 — Running it locally

## What you'll learn
- How the whole stack is wired together with Docker Compose.
- The day-to-day workflow: bring it up, push load, watch it work, tear it down.
- How to benchmark it (the simulator is the load test).
- The gotchas we actually hit, so you don't lose time on them.

## Prerequisites

- **Docker + Docker Compose** (v2).
- **Go** on your PATH (this repo was built with Go at `/usr/local/go/bin`) — only needed to
  run the simulator on the host; the services run in containers.

## One-time: what `make up` starts

```bash
make up      # docker compose up -d --build
```

This builds one image (all four Go binaries) and starts eight containers:

| Container | Port (host) | Purpose |
|-----------|-------------|---------|
| `redpanda` | 19092 | Kafka-API broker |
| `topics-init` | — | one-shot: creates `gps-pings` (6 partitions), alert topics |
| `timescaledb` | **5433** | Postgres + PostGIS + Timescale (init SQL runs on first boot) |
| `redis` | 6379 | live-position Pub/Sub |
| `ingestion` | 50051 (gRPC), 2112 (metrics) | gRPC front door |
| `processor` | 2113 (metrics) | geofencing + speed + writes |
| `dashboard` | 8080 | REST + WebSocket + map |
| `prometheus` | 9090 | metrics scraping |
| `grafana` | 3000 | dashboards (anonymous admin) |

Services connect to each other by **service name** on the Compose network
(`redpanda:9092`, `timescaledb:5432`, `redis:6379`). Host ports are only for you to poke at.

## Push load in

The simulator runs on the **host** and connects to the ingestion container's gRPC port:

```bash
make simulate VEHICLES=1000          # 1000 vehicles, 1 ping/sec each
make simulate VEHICLES=20000         # crank it up to find the limit
```

Then watch:
- **Map:** <http://localhost:8080> — vehicles moving, geofences, live alerts.
- **Grafana:** <http://localhost:3000> → "Fleet Telemetry" dashboard.
- **Prometheus:** <http://localhost:9090> — raw metrics / ad-hoc queries.

Quick end-to-end check from the CLI:

```bash
curl -s localhost:8080/api/v1/vehicles | jq length          # vehicles seen recently
curl -s 'localhost:8080/api/v1/alerts?type=all' | jq length # recent alerts
curl -s localhost:2113/metrics | grep processor_events_written_total
```

## Tear down

```bash
make down                 # stop containers (keeps the DB volume)
docker compose down -v    # also wipe the TimescaleDB volume (fresh DB next time)
```

## Benchmarking (the simulator is the load test)

There's no separate load tool — raise `VEHICLES` and watch the metrics. The questions to
answer, from the plan:

1. **How many vehicles before Kafka lag grows without bound?**
   Watch `rate(processor_pings_consumed_total[1m])` vs the simulator's send rate, and Redpanda
   consumer-group lag (`rpk group describe fleet-processor --brokers localhost:19092`). When
   consume rate can't keep up with produce rate, lag climbs — that's your ceiling for a single
   processor. Add processor replicas (up to 6, the partition count) to push it higher.
2. **End-to-end latency (ping → alert)?**
   Compare a ping's `timestamp_unix_ms` to when its alert row lands. Sub-second locally at
   moderate load.
3. **Query performance over time?**
   Run the same `/vehicles/{id}/history` query against 1 day / 1 week / 1 month of data (let it
   run, or backfill). Chunk exclusion should keep it roughly flat per unit of time scanned.

Record your numbers here as you find them — that table is exactly what an interviewer wants to
see.

## Gotchas we actually hit (and fixed)

1. **Port 5432 already in use.** A local Postgres was bound to 5432, so TimescaleDB couldn't
   start. Fix: the compose file maps TimescaleDB to host **5433** (`5433:5432`). Containers
   still talk to it on 5432 internally.
2. **Pings consumed but not written (the big one).** The processor published each live
   position to Redis **synchronously** in the hot path. When Redis was briefly unresolvable,
   every ping blocked on connection retries and DB writes stalled — 3300 consumed, ~38 written,
   zero errors logged. Fix: live publishing moved onto a **buffered channel + goroutine** with
   a 2 s timeout, dropping on overflow. Lesson: *never put a best-effort, external-dependency
   call in a hot path without a timeout and a bypass.* See
   [ADR-0004](decisions/0004-redis-hot-path-stall.md).
3. **h3-go needs cgo.** The geofencing library isn't pure Go, so the Docker build enables CGO
   and the runtime image is `debian-slim` (has libc), not `scratch`.

## How the pieces map to the docs

Each container corresponds to a phase doc: simulator → [01](01-simulator.md), ingestion →
[03](03-ingestion.md), processor → [04](04-processing-geospatial.md), TimescaleDB →
[05](05-timescaledb.md), dashboard → [06](06-dashboard.md), Prometheus/Grafana →
[08](08-observability.md).
