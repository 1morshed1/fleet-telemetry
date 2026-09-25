# 01 — The Simulator

## What you'll learn
- How to model movement on the Earth's surface (the GPS math).
- Why great-circle distance (haversine) beats naive distance on lat/lng.
- Go concurrency for a fleet: one owner per vehicle, per-vehicle RNG, one gRPC stream.
- How the simulator doubles as the project's load test.

## The problem

We have no real trucks, but we need a realistic, controllable firehose of GPS pings to build
and stress the rest of the system. The simulator must:

- Produce **plausible motion** — vehicles that drive along, turn, speed up and slow down.
- Produce **noisy data** — real GPS wobbles by a few meters; real speed sensors are off by a
  few km/h. If our data were perfect, we'd never test the messy real-world path.
- **Scale on demand** — turn a knob from 100 to 50,000 vehicles to find the breaking point.
- Be **reproducible** — same seed, same fleet, so bugs are repeatable.

## The GPS math (`internal/geo`)

Two operations power the whole simulation, and they live in
[`internal/geo/geo.go`](../internal/geo/geo.go) so the processor can reuse them.

### Moving a point — dead reckoning

Given a start position, a heading, and a distance, where do you end up?

```go
newLat = lat + (distanceKm / 111.0) * cos(heading)
newLng = lng + (distanceKm / (111.0 * cos(lat))) * sin(heading)
```

- One degree of **latitude** is ~111 km everywhere.
- One degree of **longitude** shrinks toward the poles — a degree of longitude in northern
  Norway covers far less ground than at the equator. That's the `* cos(lat)` correction. Skip
  it and your vehicles drift sideways the further north they are.
- `heading` is a compass bearing: 0 = north, 90 = east, measured clockwise. North should move
  you along latitude → `cos(heading)`; east along longitude → `sin(heading)`.

This is a **flat-earth approximation**, which is fine because each tick moves the vehicle only
tens of meters. Over such tiny steps the curvature of the Earth is negligible.

### Measuring distance — haversine

To recompute speed from two positions (the processor does this), we need the real distance
between two lat/lng points on a sphere:

```go
a = sin²(Δlat/2) + cos(lat1)·cos(lat2)·sin²(Δlng/2)
distance = 2R · atan2(√a, √(1−a))      // R = 6371 km
```

**Why not just Pythagoras on lat/lng?** Because longitude degrees aren't a fixed length, so
`√(Δlat² + Δlng²)` is simply wrong — it overestimates east-west distance the further you are
from the equator. Haversine is the correct great-circle distance and it's cheap.

We sanity-check both functions in [`geo_test.go`](../internal/geo/geo_test.go): London→Paris
comes out ~343 km, and "move 1 km north then measure" returns ~1 km.

## The vehicle state machine (`internal/simulator`)

Each [`Vehicle`](../internal/simulator/vehicle.go) holds position, speed, heading, and a
**route type** (highway / urban / rural). Every `Tick`:

1. **Maybe turn.** Urban vehicles turn 25% of ticks (intersections), highway ones 2% (you
   don't turn much on a motorway). A turn nudges heading by up to ±45°.
2. **Maybe change speed**, staying inside the route's range (highway 90–130, urban 20–60,
   rural 50–90 km/h) — with a deliberate ~2% chance of overshooting the range so **speeding
   alerts actually fire** in demos.
3. **Advance** using `geo.Move(speed × interval)`.
4. **Add jitter** — ±~5 m of position noise and ±~5 km/h of reported-speed noise, because
   real sensors are imperfect.

### Concurrency decisions

- **Per-vehicle RNG.** Go's global `rand` functions take a lock; calling them from thousands
  of goroutines would serialize on that lock. Each `Vehicle` owns its own `*rand.Rand` seeded
  deterministically, so there's no shared state and no contention.
- **Reproducibility.** Vehicle *i* is seeded `base + i`. Same base seed ⇒ identical fleet and
  identical ticks (verified by `TestReproducibleBySeed`). Invaluable when chasing a bug.
- **Domain type, not wire type.** `Tick` returns a plain `simulator.Ping`, not the generated
  protobuf `GPSPing`. The domain model shouldn't depend on wire code; the `cmd` layer converts
  `Ping → GPSPing` when sending. This keeps the simulation testable without gRPC.

## Streaming to ingestion (`cmd/simulator`)

[`cmd/simulator/main.go`](../cmd/simulator/main.go) is the gRPC **client**:

- Opens **one** bidirectional stream (`StreamPings`) and reuses it for the whole run — no
  per-ping connection cost.
- **One sender goroutine** owns `stream.Send`; **one reader goroutine** owns `stream.Recv`.
  This matches gRPC's rule: a stream tolerates one concurrent sender and one concurrent
  receiver, but not more. The reader also *must* keep draining acks, or the server's send side
  would eventually block.
- Tracks `sent / accepted / rejected` with atomics and logs throughput every 5 s. `rejected`
  is the **backpressure** signal — the server telling us it can't keep up (see
  [03](03-ingestion.md)).
- Cancels cleanly on Ctrl-C, closing the send side and letting final acks drain.

## The simulator IS the load test

There's no separate load-testing tool. To benchmark the pipeline you just raise the vehicle
count:

```bash
make simulate VEHICLES=1000      # gentle
make simulate VEHICLES=20000     # find where Kafka lag starts to grow
```

The questions to answer (recorded in [07-running-locally.md](07-running-locally.md)):
- How many vehicles before consumer lag grows without bound?
- End-to-end latency from ping emission to alert?

## Try it

The simulator needs the ingestion server running (next doc). Once `make up` is going:

```bash
make simulate VEHICLES=500
# simulator: created 500 vehicles around (52.5200, 13.4050)
# simulator: sent=2500 (500/s) accepted=2500 rejected=0
```
