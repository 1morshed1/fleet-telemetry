# ADR-0004: Keep best-effort external calls off the hot path

- **Status:** Accepted
- **Date:** 2026-09-25

## Context

During the first full end-to-end run, the pipeline appeared healthy — 3300 pings consumed,
alerts firing — but only ~38 rows reached `gps_events`, with **zero** database errors logged.
Something was silently swallowing the throughput.

## Investigation

The processor's `handle()` did, per ping:
1. H3 geofence lookup (in-memory, fast)
2. speed calc (in-memory, fast)
3. optional alert inserts (rare)
4. **`rdb.Publish(...)` to Redis — synchronous, in the hot path**
5. return the event to be batched and written

The processor logs were full of:

```
redis: connection pool: failed to dial after 5 attempts: dial tcp: lookup redis on 127.0.0.11:53: server misbehaving
```

Redis's DNS name was intermittently unresolvable at startup. Because the publish was
synchronous, **every ping** blocked on go-redis's dial + retry loop (seconds each). The single
processing goroutine spent all its time waiting on Redis, so it drained the ping channel and
flushed to TimescaleDB at a crawl. No error counter moved because the *database* was fine — the
stall was upstream of it.

## Decision

Move live publishing **off the hot path**:

- `handle()` now does a **non-blocking** send onto a buffered `liveCh` (`select { case
  liveCh <- pos: default: drop }`).
- A dedicated `runPublisher` goroutine drains `liveCh` and calls `rdb.Publish` with a **2 s
  timeout** per call.
- The Redis client is configured to **fail fast** (2 s dial/read/write timeouts,
  `MaxRetries: 1`).

Live positions are best-effort — the durable copy is already in TimescaleDB — so dropping them
under pressure is the correct behavior. Two metrics make the drops visible:
`processor_live_dropped_total` and `processor_live_publish_errors_total`.

## Consequences

- The hot path (enrich → batch → DB write) can no longer be stalled by a slow or missing
  Redis. DB write rate now tracks the consume rate.
- **General principle (the real lesson):** any call to an external system that is *best-effort*
  must not sit synchronously in a hot path. Give it (a) a timeout, and (b) a bypass — a buffer
  it can overflow into, or a circuit breaker — so its failure degrades one feature instead of
  halting the pipeline.
- This mirrors the design already used elsewhere: ingestion buffers to Kafka behind a bounded
  queue ([03](../03-ingestion.md)); the dashboard hub drops updates to slow WebSocket clients
  ([06](../06-dashboard.md)). Same pattern, applied consistently.
