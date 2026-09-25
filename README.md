# Fleet Telemetry — Real-Time IoT Fleet Management & Geospatial Dashboard

A high-throughput system that ingests GPS pings from a simulated vehicle fleet, detects
**geofence violations** and **speeding** in real time, stores everything in a time-series
database, and shows it live on a map.

This repo is built as a **learning project**. Every phase has a companion document in
[`docs/`](docs/) that explains *what* was built, *why* the decisions were made, and the
trade-offs behind them. Read the docs in order — they are the point.

```
Simulator ──gRPC stream──▶ Ingestion ──▶ Kafka ──▶ Processor ──▶ TimescaleDB
  (Go)                       (Go)                     (Go)          (Postgres+
                                │                       │            PostGIS+
                                └──────▶ Redis ◀────────┘            Timescale)
                                          │                              ▲
                                          │                              │
                                    Dashboard API (Go: WebSocket + REST) ┘
                                          │
                                     Web dashboard (Leaflet map)
```

## Components

| Service      | Language | Role |
|--------------|----------|------|
| `simulator`  | Go       | Simulates N vehicles moving on routes, streams GPS pings over gRPC. |
| `ingestion`  | Go       | gRPC bidirectional stream server. Batches pings, produces to Kafka. |
| `processor`  | Go       | Consumes Kafka. Runs geofencing (H3) + speed detection. Writes TimescaleDB, publishes live positions to Redis. |
| `dashboard`  | Go       | REST for historical queries + WebSocket for live positions (fed by Redis Pub/Sub). |
| `web`        | HTML/JS  | Leaflet map showing live vehicles, geofences, alerts. |

Infrastructure (via `docker-compose`): **Kafka** (Redpanda), **TimescaleDB** (Postgres +
PostGIS + Timescale), **Redis**, **Prometheus**, **Grafana**.

## Quick start

```bash
# 1. Start infrastructure + services
make up

# 2. Start the simulator (streams pings into the system)
make simulate VEHICLES=1000

# 3. Open the dashboard
open http://localhost:8080          # live map
open http://localhost:3000          # Grafana (admin/admin)
```

See [`docs/07-running-locally.md`](docs/07-running-locally.md) for details.

## Documentation map

Start here → [`docs/README.md`](docs/README.md)

| Doc | Topic |
|-----|-------|
| [00-architecture.md](docs/00-architecture.md) | The whole system, data flow, and why it is shaped this way. |
| [01-simulator.md](docs/01-simulator.md) | Modeling vehicle movement, GPS math, goroutine-per-vehicle. |
| [02-protobuf-grpc.md](docs/02-protobuf-grpc.md) | Protobuf schema, gRPC bidirectional streaming. |
| [03-ingestion.md](docs/03-ingestion.md) | Batching, backpressure, Kafka producer tuning. |
| [04-processing-geospatial.md](docs/04-processing-geospatial.md) | H3 hexagonal indexing, geofencing, speed detection. |
| [05-timescaledb.md](docs/05-timescaledb.md) | Hypertables, continuous aggregates, compression, PostGIS. |
| [06-dashboard.md](docs/06-dashboard.md) | REST + WebSocket, Redis Pub/Sub fan-out. |
| [07-running-locally.md](docs/07-running-locally.md) | Docker Compose, Makefile, day-to-day workflow. |
| [08-observability.md](docs/08-observability.md) | Prometheus metrics, Grafana dashboards. |
| [09-kubernetes-gitops.md](docs/09-kubernetes-gitops.md) | K8s manifests, kustomize, ArgoCD. |
| [decisions/](docs/decisions/) | Architecture Decision Records (ADRs) — the *why* behind big choices. |

## Status

Built phase by phase. See git history and the per-phase docs for what is done.
