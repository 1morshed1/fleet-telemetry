# 03 — The Ingestion Service

## What you'll learn
- Why we **batch** writes to Kafka instead of one message per ping.
- What **backpressure** is and how to implement it with a bounded queue + non-blocking send.
- Why we partition Kafka by `vehicle_id`, and what that buys us.
- How the gRPC server, an in-memory queue, and a batcher goroutine fit together.

## The problem

The ingestion service is the front door for the entire firehose. It must:

1. Accept thousands of pings/sec over gRPC without falling over.
2. Get them into Kafka **efficiently** — naive "one Kafka write per ping" would be dominated
   by per-request overhead.
3. **Never OOM.** If Kafka is momentarily slow, the service must not buffer pings in memory
   without limit. It has to push back on senders instead.

## The shape

```
gRPC stream(s) ──▶ [ bounded queue ] ──▶ batcher goroutine ──▶ Kafka
   Recv/Send          (channel)            accumulate 500        (one WriteMessages
   per ping        backpressure here        or 100ms              call per batch)
```

Source: [`internal/ingestion/server.go`](../internal/ingestion/server.go),
[`cmd/ingestion/main.go`](../cmd/ingestion/main.go).

## Idea 1: Batching

Every network write to Kafka has fixed overhead (a request frame, a round-trip, a broker ack).
If you pay that per ping, throughput collapses. So we **amortize** it: buffer pings and send
many in one `WriteMessages` call.

The batcher flushes when **either**:
- the batch reaches **500 pings** (size trigger), or
- **100 ms** elapses since the last flush (time trigger).

The time trigger matters: at low traffic you'd otherwise wait forever to reach 500 and add
latency. "N items **or** T milliseconds, whichever comes first" is the canonical batching
pattern — you'll see it in log shippers, metrics agents, and database bulk loaders everywhere.

```go
case ping := <-s.queue:
    batch = append(batch, ping)
    if len(batch) >= s.cfg.MaxBatch { flush() }   // size trigger
case <-ticker.C:
    flush()                                        // time trigger
```

We reuse the batch's backing array (`batch = batch[:0]`) so steady-state batching does zero
allocations for the slice itself.

## Idea 2: Backpressure

The queue between the gRPC handlers and the batcher is a **bounded** Go channel
(`make(chan *GPSPing, 100_000)`). Bounded is the whole point.

When a ping arrives we try a **non-blocking** enqueue:

```go
select {
case s.queue <- ping:
    return true    // accepted
default:
    return false   // queue full → backpressure
}
```

If the queue is full (Kafka can't keep up, so the batcher isn't draining fast enough), we
**don't block and we don't grow memory**. We reply `PingAck{accepted: false}`. That travels
back over the bidirectional stream to the client, which sees `rejected++` and can slow down.

This is why the RPC is **bidirectional** ([02](02-protobuf-grpc.md)): the server needs a
channel to say "slow down" *while* the client is still sending. Without backpressure, an
overloaded service either OOMs (unbounded buffer) or blocks all senders (bounded, blocking
buffer). Bounded + non-blocking + a reject signal is the healthy third option.

> Real devices would honor the reject by dropping to a lower sample rate or buffering locally.
> Our simulator just counts rejections, which is enough to *see* backpressure working.

## Idea 3: Partition by `vehicle_id`

Kafka topics are split into **partitions**; ordering is only guaranteed *within* a partition.
We set each message's key to `vehicle_id` and use the **Hash balancer**, so every ping for a
given vehicle always lands on the same partition, in order.

Why that matters downstream: the processor computes speed from **consecutive** pings of the
same vehicle. If a vehicle's pings were scattered across partitions and consumers, "previous
ping" would be meaningless. Keying by `vehicle_id` guarantees each vehicle's timeline stays
intact and gets processed by exactly one consumer. See
[`internal/kafka/kafka.go`](../internal/kafka/kafka.go).

## Serialization

Batch values are **protobuf-encoded** `GPSPing`s, not JSON — same reasoning as the wire format
in [02](02-protobuf-grpc.md): smaller and faster. The processor unmarshals the same protobuf.
The Kafka message key is the raw `vehicle_id` bytes (used for partitioning).

## Observability built in

The service exposes Prometheus metrics on `:2112/metrics`
([08-observability.md](08-observability.md)):

| Metric | Tells you |
|--------|-----------|
| `ingestion_pings_received_total` | inbound rate |
| `ingestion_pings_rejected_total` | **backpressure** — if this climbs, you're overloaded |
| `ingestion_pings_produced_total` | successful Kafka writes |
| `ingestion_batches_flushed_total` + `ingestion_batch_size` | how well batching is working |
| `ingestion_queue_depth` | how full the buffer is (headroom before backpressure) |
| `ingestion_kafka_write_errors_total` | broker trouble |

`received - produced - rejected` should hover near the in-flight batch size. A growing gap
means Kafka writes are failing.

## Testing without Kafka

The server takes a small `writer` interface, not a concrete `*kafka.Writer`. Tests inject a
`fakeWriter` and assert the batcher flushes by size, flushes by interval, keys messages by
`vehicle_id`, and rejects when the queue is full — all with no broker running. See
[`server_test.go`](../internal/ingestion/server_test.go). Depending on a narrow interface
instead of a concrete type is what makes the hot path unit-testable.

## Scaling note

The service is stateless — it just forwards to Kafka. To handle more load, run more replicas
behind the gRPC load balancer (Kubernetes HPA on CPU or on `ingestion_queue_depth`). Kafka
partitions and consumer groups on the *other* side let the processor scale independently.
