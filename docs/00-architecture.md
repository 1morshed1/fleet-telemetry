# 00 — Architecture

## What you'll learn
- How the pieces of a real-time telemetry pipeline fit together.
- Why each stage exists and what would break without it.
- The vocabulary: ingestion, buffering, stream processing, time-series storage, fan-out.

## The problem

Imagine 10,000 delivery trucks, each reporting its GPS position once per second. That's
10,000 events per second, forever. You need to:

1. **Receive** them without dropping any, even in bursts.
2. **Decide fast**: did a truck enter a restricted zone? Is it speeding?
3. **Store** the history so you can answer "where was truck 42 last Tuesday?"
4. **Show** the live picture on a map, updating in real time.

A single program with a `for` loop and a database can't do this — it would fall over. The
solution is a **pipeline** of specialized stages, each doing one job well, connected by a
durable buffer so no stage can overwhelm the next.

## The pipeline

```
┌───────────┐   gRPC     ┌───────────┐  produce   ┌─────────┐  consume   ┌───────────┐
│ Simulator │──────────▶ │ Ingestion │──────────▶ │  Kafka  │──────────▶ │ Processor │
│           │  stream    │  (gRPC)   │  (batched) │(Redpanda)│           │ (Go)      │
└───────────┘            └───────────┘            └─────────┘            └─────┬─────┘
                                                                               │
                              ┌────────────────────────────────────────────────┤
                              │                                                  │
                              ▼ write                                            ▼ publish
                     ┌────────────────┐                                  ┌────────────┐
                     │  TimescaleDB   │                                  │   Redis    │
                     │ (history +     │                                  │ (Pub/Sub)  │
                     │  geofences)    │                                  └─────┬──────┘
                     └───────┬────────┘                                        │
                             │ REST query                              WS subscribe
                             │                                                 │
                             └──────────────▶ ┌───────────────┐ ◀─────────────┘
                                              │ Dashboard API │
                                              │ (REST + WS)   │
                                              └───────┬───────┘
                                                      │
                                                      ▼
                                              ┌───────────────┐
                                              │ Web dashboard │
                                              │ (Leaflet map) │
                                              └───────────────┘
```

## Why each stage exists

### Simulator
We have no real trucks. The simulator stands in for thousands of GPS devices. It's also our
**load test** — turning the vehicle count up is how we find the breaking point. Details:
[01-simulator.md](01-simulator.md).

### Ingestion (gRPC)
The "front door." It holds one long-lived streaming connection per device instead of a new
HTTP request per ping (which would drown in TCP handshakes). Its second job is to **batch**
pings and hand them to Kafka efficiently, and to push **backpressure** to devices when the
system is overloaded rather than running out of memory. Details: [03-ingestion.md](03-ingestion.md).

### Kafka (Redpanda) — the buffer
This is the shock absorber. Ingestion writes as fast as pings arrive; the processor reads
as fast as it can. Kafka sits between them so a slow processor never blocks ingestion, and a
crash on either side loses nothing (messages are durable on disk). Partitioning by
`vehicle_id` keeps each vehicle's pings **in order**. We use Redpanda, a Kafka-compatible
broker that's a single binary — see
[ADR-0003](decisions/0003-redpanda-vs-kafka.md).

### Processor
The brain. For each ping it:
- Converts lat/lng to an **H3 hex cell** and checks it against precomputed geofence cells
  (O(1) instead of scanning polygon edges).
- Computes speed between consecutive pings and flags speeding.
- Writes the enriched event to TimescaleDB.
- Publishes the live position to Redis so the dashboard can fan it out.

The original plan called for Apache Spark here; we use Go instead and explain why in
[ADR-0002](decisions/0002-go-processor-vs-spark.md). The geospatial ideas are identical.
Details: [04-processing-geospatial.md](04-processing-geospatial.md).

### TimescaleDB — the history
Postgres with two superpowers: **PostGIS** (geometry types + spatial queries) and
**TimescaleDB** (auto-partitioning by time, continuous aggregates, compression). This is
where "show me the route for the last hour" queries stay fast even as data grows. Details:
[05-timescaledb.md](05-timescaledb.md).

### Redis — live fan-out
The processor publishes each live position to a Redis Pub/Sub channel. The dashboard API
subscribes once and forwards to every connected browser. Redis decouples "how many browsers
are watching" from the processor — the processor publishes once regardless.

### Dashboard API + Web
REST endpoints answer historical questions from TimescaleDB. A WebSocket endpoint streams
live positions from Redis to the browser. The web page draws it all on a Leaflet map.
Details: [06-dashboard.md](06-dashboard.md).

## The two golden ideas

If you take away two things from this architecture:

1. **A durable buffer (Kafka) between producers and consumers decouples their speeds.**
   Each side scales independently and neither can crash the other.
2. **Precompute to make hot-path work O(1).** H3 turns geofencing from "loop over polygon
   edges per ping" into "one hash lookup." Continuous aggregates turn "scan a million rows"
   into "read a precomputed summary." The pattern — *do expensive work once, ahead of
   time* — repeats everywhere in high-throughput systems.

## What's deliberately simplified

This runs on one machine via Docker Compose. In production you'd run Kafka/Timescale as
clusters, the Go services as horizontally-scaled K8s deployments behind an HPA, and deploy
via GitOps. We include K8s manifests and an ArgoCD setup
([09-kubernetes-gitops.md](09-kubernetes-gitops.md)) to show the shape of that, without
requiring a cloud account to learn from this repo.
