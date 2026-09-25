# 02 — Protobuf & gRPC streaming

## What you'll learn
- What Protobuf is and why we use it instead of JSON for the wire format.
- What gRPC bidirectional streaming is and why it fits high-frequency telemetry.
- How code generation turns a `.proto` file into Go types and a typed client/server.

## The problem

Ten thousand devices each send a small message every second. Two questions decide the design:

1. **How do we encode each message?** JSON is human-readable but bulky and slow to
   parse. At tens of thousands of messages/sec that overhead is real CPU and bandwidth.
2. **How do we transport them?** One HTTP request per ping means one TCP/TLS handshake per
   ping — enormous waste for tiny payloads sent constantly.

## The answers

### Protobuf for encoding

[Protocol Buffers](https://protobuf.dev) is a binary serialization format. You describe your
messages once in a `.proto` schema; a compiler generates struct code for every language.

Why it wins here:
- **Small:** fields are tagged by number, not name, so `{"latitude":52.5}` becomes a couple
  of bytes. Typically 3–5× smaller than JSON.
- **Fast:** parsing is a tight loop over known field numbers, no string matching. ~10× faster
  than JSON encode/decode.
- **Typed + versioned:** the schema is the contract. Add a field with a new number and old
  and new code still interoperate (forward/backward compatible).

Our schema ([`proto/fleet/v1/fleet.proto`](../proto/fleet/v1/fleet.proto)):

```protobuf
message GPSPing {
  string vehicle_id = 1;
  double latitude = 2;
  double longitude = 3;
  double speed_kmh = 4;
  double heading = 5;
  int64 timestamp_unix_ms = 6;
}
```

Each field's **number** (`= 1`, `= 2`, …) is what goes on the wire — never reuse or renumber
them once shipped. That's the whole compatibility trick.

### gRPC bidirectional streaming for transport

[gRPC](https://grpc.io) is an RPC framework built on HTTP/2 and Protobuf. HTTP/2 gives us one
long-lived, multiplexed connection. gRPC adds **streaming** RPCs on top.

```protobuf
service FleetIngestion {
  rpc StreamPings(stream GPSPing) returns (stream PingAck);
}
```

The two `stream` keywords make this **bidirectional streaming**: the client keeps sending
`GPSPing`s and the server keeps sending `PingAck`s, both over the same connection, for as
long as the device is online. The four RPC shapes:

| Shape | Signature | Example |
|-------|-----------|---------|
| Unary | `rpc F(Req) returns (Resp)` | normal request/response |
| Server streaming | `rpc F(Req) returns (stream Resp)` | subscribe to updates |
| Client streaming | `rpc F(stream Req) returns (Resp)` | upload a file in chunks |
| **Bidirectional** | `rpc F(stream Req) returns (stream Resp)` | **our telemetry feed** |

Why bidirectional and not just client-streaming (send pings, get one final ack)? Because we
want the server to talk back **while** the stream is open — specifically to say
`accepted: false` when it's overloaded, so the device can slow down. That's backpressure, and
it needs a return stream. More in [03-ingestion.md](03-ingestion.md).

## Code generation

We don't hand-write the wire code. The toolchain:

- **`buf`** — a modern front-end for Protobuf. It lints the schema and drives generation.
  Config: [`buf.yaml`](../buf.yaml) (lint/breaking rules) and
  [`buf.gen.yaml`](../buf.gen.yaml) (which plugins, where output goes).
- **`protoc-gen-go`** — generates the message structs → `fleet.pb.go`.
- **`protoc-gen-go-grpc`** — generates the client and server interfaces → `fleet_grpc.pb.go`.

Regenerate with:

```bash
make proto      # runs: buf lint && buf generate
```

This produces, in `proto/fleet/v1/`:
- `GPSPing`, `PingAck` Go structs (with efficient marshal/unmarshal).
- `FleetIngestionClient` — what the simulator uses to call the server.
- `FleetIngestionServer` — the interface the ingestion service implements.
- `FleetIngestion_StreamPingsServer/Client` — the typed stream handles you call
  `.Send()` / `.Recv()` on.

## A note on naming

Buf's standard lint rules want RPC request/response types named
`StreamPingsRequest`/`StreamPingsResponse` and services suffixed with `Service`. We
deliberately keep the domain names `GPSPing`, `PingAck`, and `FleetIngestion` because they
read better in a learning codebase, and we disabled exactly those four lint rules in
`buf.yaml` with a comment explaining why. Turning a rule off *on purpose, with a reason* is
fine; leaving lint noise is not.

## Where this goes next

- The **simulator** ([01](01-simulator.md)) is the gRPC *client* — it opens the stream and
  sends pings.
- The **ingestion service** ([03](03-ingestion.md)) is the gRPC *server* — it implements
  `StreamPings`, batches what it receives, and forwards to Kafka.
