# ADR-0001: Deviations from the original plan

- **Status:** Accepted
- **Date:** 2026-09-25

## Context

The project started from `plan.md`, an ambitious spec calling for Go + gRPC ingestion,
Kafka, **Apache Spark Structured Streaming** for processing, TimescaleDB + PostGIS + H3 for
storage/geospatial, a Go dashboard, and full Kubernetes + ArgoCD + Ansible + Terraform
deployment targeting **millions of pings/sec**.

That plan is a great *north star*, but building all of it exactly as written would produce
something that is hard to run, hard to learn from, and mostly untested. The goal of this
repo is a **working system you can run on a laptop and actually understand**, while still
demonstrating every core concept the plan is about.

## Decisions

The following deviations were made deliberately. Each has its own detail where noted.

| Plan says | We do | Why |
|-----------|-------|-----|
| Apache Spark for processing | **Go consumer** for processing | Reliability, testability, single-language repo, and it runs instantly. The H3 geofencing + speed logic is the real lesson and is identical. See [ADR-0002](0002-go-processor-vs-spark.md). Reference PySpark is documented in [04](../04-processing-geospatial.md). |
| Apache Kafka + Zookeeper | **Redpanda** (Kafka API compatible) | Single binary, no JVM, no Zookeeper. Same client code. See [ADR-0003](0003-redpanda-vs-kafka.md). |
| H3 inside Spark (Python `h3`) | **`uber/h3-go`** in the Go processor | Keeps everything one language; identical hex math. |
| Redis Pub/Sub populated "by the ingestion service" | populated by the **processor** | The processor is where a ping becomes an enriched, geofenced position — that's what the live map should show. |
| Multiple Go modules / repos incl. a separate GitOps repo | **single Go module**, GitOps manifests in `deploy/` | Easier to read and build as one unit. In production the GitOps repo is separate; we note where the split would go. |
| Millions of pings/sec target | **thousands/sec on a laptop**, architecture that scales | The bottleneck ideas (batching, partitioning, backpressure, O(1) geofencing) are what matter and are all present. Real numbers are benchmarked in the running-locally doc. |
| Ansible + Terraform for cluster/VM provisioning | **out of scope** (documented, not built) | Provisioning real VMs needs a cloud account and adds no conceptual value to the streaming/geospatial core. The K8s + ArgoCD manifests show the deployment shape. |
| Spark broadcast variable for geofence cells | in-memory map refreshed periodically in the Go processor | Same idea (load geofences once, keep in memory), simpler mechanism. |

## Consequences

- **Positive:** The whole system builds with `go build`, runs with `docker compose up`, and
  every stage is unit-testable. A reader can trace a ping end to end in one language.
- **Positive:** Faster feedback loop → the docs can show real, reproducible behavior.
- **Trade-off:** We don't demonstrate Spark's distributed shuffle/stateful operators
  first-hand. We compensate with a documented, runnable PySpark reference of the same job.
- **Trade-off:** Laptop-scale throughput. The design scales horizontally (stateless Go
  services + partitioned Kafka), but we don't prove million/sec numbers here.

## Guiding principle

> Prefer a smaller system that **runs and is understood** over a larger one that only
> exists on paper. Document every place where the "real production" choice would differ.
