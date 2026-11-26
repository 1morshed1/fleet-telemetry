# 08 — Observability

## What you'll learn
- The difference between **metrics**, **logs**, and **traces**, and which we use where.
- How Prometheus **scrapes** Go services and how Grafana turns that into graphs.
- Which metrics actually matter for a streaming pipeline (the "golden signals" here).
- How the bug in [ADR-0004](decisions/0004-redis-hot-path-stall.md) would look on a dashboard.

## The problem

A distributed pipeline is opaque. When "only 38 of 3300 pings got written," you can't `printf`
your way to the answer across four services. You need **numbers over time** you can correlate.
That's observability, classically three pillars:

| Pillar | Answers | Here |
|--------|---------|------|
| **Metrics** | "how much / how fast / how many errors" | Prometheus + Grafana ✅ |
| **Logs** | "what exactly happened at 10:47:02" | container stdout (`docker compose logs`) ✅ |
| **Traces** | "where did this one request spend its time" | Jaeger — noted, not wired (see below) |

## Metrics: Prometheus pull model

Each Go service exposes a `/metrics` endpoint (via `promhttp`) that prints its current counter
and gauge values in Prometheus's text format. Prometheus **scrapes** those endpoints every 5 s
([`deploy/prometheus/prometheus.yml`](../deploy/prometheus/prometheus.yml)) and stores the
time series. This *pull* model means services don't need to know Prometheus exists — they just
expose numbers; Prometheus comes and gets them.

- Ingestion → `:2112/metrics`
- Processor → `:2113/metrics`

Grafana ([`deploy/grafana`](../deploy/grafana)) auto-provisions the Prometheus datasource and
loads the **Fleet Telemetry** dashboard on startup — no clicking. Open
<http://localhost:3000>.

## The metrics that matter

We instrument the two things that break in streaming systems: **throughput** and **lag/errors**.

### Ingestion
| Metric | Type | Why you watch it |
|--------|------|------------------|
| `ingestion_pings_received_total` | counter | inbound rate (`rate(...[1m])`) |
| `ingestion_pings_produced_total` | counter | did they make it to Kafka? |
| `ingestion_pings_rejected_total` | counter | **backpressure** — climbing = overloaded |
| `ingestion_queue_depth` | gauge | headroom before backpressure |
| `ingestion_batch_size` | histogram | is batching effective? (avg = sum/count) |
| `ingestion_kafka_write_errors_total` | counter | broker trouble |

### Processor
| Metric | Type | Why you watch it |
|--------|------|------------------|
| `processor_pings_consumed_total` | counter | consume rate — must keep up with produce rate |
| `processor_events_written_total` | counter | are enriched events reaching the DB? |
| `processor_speed_alerts_total` / `_geofence_alerts_total` | counter | detection working |
| `processor_db_errors_total` | counter | DB write failures |
| `processor_live_dropped_total` / `_live_publish_errors_total` | counter | Redis fan-out health |

### The golden rule for a pipeline

**Watch consume-rate vs produce-rate.** If `rate(processor_pings_consumed_total)` can't match
the ingestion produce rate, Kafka consumer lag grows and the system falls behind. That single
comparison is the headline health signal. Confirm lag directly with:

```bash
rpk group describe fleet-processor --brokers localhost:19092
```

## How the ADR-0004 bug looks on the dashboard

This is why the instrumentation earns its keep. During the stall:

- `rate(processor_pings_consumed_total)` was healthy (pings were arriving),
- but `rate(processor_events_written_total)` was **near zero**,
- and `processor_db_errors_total` was **flat at zero**.

"Consuming fine, not writing, no DB errors" points away from the database and toward *something
between consume and write* — which is exactly where the synchronous Redis publish was. A
dashboard turns a mystery into a two-panel diff. (After the fix, the two rates track each other,
and `processor_live_dropped_total` would rise instead if Redis were slow — degrading the right
feature.)

## Logs

Structured-ish stdout from each service, viewed with `docker compose logs -f <service>`. This is
how we *saw* the actual Redis DNS error text. Metrics tell you **that** something is wrong and
where to look; logs tell you **what**. You need both.

## Traces (documented, not wired)

The spec lists **Jaeger** for distributed tracing and **Loki** for log aggregation. They're the
natural next step but add containers and instrumentation without changing the core lessons, so
they're left as an exercise:

- **Tracing** would add OpenTelemetry spans across simulator → ingestion → Kafka → processor →
  DB, so you could see the end-to-end latency of a single ping and where it's spent. gRPC and
  `pgx` both have OTel middleware; the Kafka hop is bridged by propagating a trace id in a
  message header.
- **Loki** would let Grafana query logs alongside metrics in one place.

## The takeaway

Instrument the **flow rates** and the **error/lag** at every stage boundary. When something
breaks, you diagnose by comparing adjacent stages' rates — the stage where the rate drops is the
culprit. That's the whole game.
