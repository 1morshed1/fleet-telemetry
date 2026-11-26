# ADR-0002: Go processor instead of Apache Spark

- **Status:** Accepted
- **Date:** 2026-09-25

## Context

The spec uses Apache Spark Structured Streaming for the geofencing and speed-detection
stage. Spark is the industry default for large-scale stream/batch processing. But for this
project it carries real costs:

- **Operational weight:** Spark needs a JVM, a driver + executors, and a cluster manager. A
  local Spark job in Docker is a heavy, slow-to-start, memory-hungry container.
- **Debuggability:** A bug in a PySpark UDF surfaces as a stack trace buried in executor
  logs. Iterating is slow.
- **Language split:** The rest of the system is Go. Adding Python + Spark means two
  toolchains, two test frameworks, two dependency systems.
- **Overkill at our scale:** Spark shines at terabytes and hundreds of cores. Our laptop
  workload (thousands of pings/sec) fits comfortably in a single Go process.

## Decision

Implement the processing stage as a **Go Kafka consumer**:

- Geofencing via `github.com/uber/h3-go` (same H3 algorithm the spec's PySpark uses).
- Speed detection by keeping the last ping per vehicle in an in-memory map and computing the
  haversine distance / time delta.
- Writes to TimescaleDB via `pgx`, publishes live positions to Redis.

Scale-out story: run more processor instances; Kafka's consumer-group protocol assigns
partitions across them. Because pings are partitioned by `vehicle_id`, all pings for a
vehicle land on one instance, so the "last ping per vehicle" state is correct without a
shared store.

## Why this is still a valid demonstration

The **hard, interesting parts are unchanged**:

- H3 hexagonal indexing for O(1) point-in-polygon — the geospatial differentiator.
- Per-vehicle stateful speed calculation (Spark calls this a windowed/stateful operator; we
  do it explicitly with a map, which makes the concept *more* visible, not less).
- Consuming from a partitioned Kafka topic with a consumer group.

## What we give up (and how we cover it)

| Give up | Mitigation |
|---------|-----------|
| First-hand Spark experience | A runnable PySpark reference job is documented in [04-processing-geospatial.md](../04-processing-geospatial.md), showing the exact same logic in Spark. |
| Distributed shuffle / exactly-once sink semantics | Documented; our consumer uses at-least-once with idempotent-friendly writes. |
| Auto-scaling executors | Kafka consumer-group rebalancing gives the same horizontal-scale property for our workload. |

## Consequences

- Fast startup, easy tests, one language, trivial local debugging.
- If a reader specifically wants Spark on their resume, the reference job + this ADR show
  they understand the trade-off — which is itself a senior signal.
