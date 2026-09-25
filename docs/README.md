# Learning Docs — read these in order

This project was built to *learn* distributed systems, streaming, and geospatial
processing. These docs are written for someone (you) who wants to understand not just the
code but the reasoning. Each one is standalone but they build on each other.

## Suggested reading order

1. **[00-architecture.md](00-architecture.md)** — Get the big picture first. What flows
   where, and why each component exists.
2. **[01-simulator.md](01-simulator.md)** — The easiest entry point: how we fake a fleet.
   Teaches Go concurrency and the GPS math you'll reuse later.
3. **[02-protobuf-grpc.md](02-protobuf-grpc.md)** — The contract between services. Why
   Protobuf, why gRPC streaming.
4. **[03-ingestion.md](03-ingestion.md)** — The high-throughput front door. Batching and
   backpressure — the ideas that let one service absorb millions of events.
5. **[04-processing-geospatial.md](04-processing-geospatial.md)** — The heart of the
   project. H3 hexagons turn "is this point in this polygon" from slow to O(1).
6. **[05-timescaledb.md](05-timescaledb.md)** — Storing time-series data at scale without
   the table exploding.
7. **[06-dashboard.md](06-dashboard.md)** — Getting data back out, live, to a browser.
8. **[07-running-locally.md](07-running-locally.md)** — How to actually run the whole thing.
9. **[08-observability.md](08-observability.md)** — Seeing inside a running system.
10. **[09-kubernetes-gitops.md](09-kubernetes-gitops.md)** — Deploying it like a team would.

## How to use these

- Each doc opens with **"What you'll learn"** and **"The problem"** so you know why it
  matters before diving in.
- Code snippets are trimmed to the interesting parts; the full source is linked.
- **Decision Records** (in [`decisions/`](decisions/)) capture the forks in the road —
  where I picked one approach over another, and what I gave up.

## Decision Records

- [ADR-0001: Deviations from the original plan](decisions/0001-deviations-from-plan.md)
- [ADR-0002: Go processor instead of Spark](decisions/0002-go-processor-vs-spark.md)
- [ADR-0003: Redpanda instead of Apache Kafka + Zookeeper](decisions/0003-redpanda-vs-kafka.md)

More ADRs are added as decisions come up. They are numbered in the order the decision was
made.
