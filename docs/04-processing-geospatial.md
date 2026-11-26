# 04 — Processing & Geospatial (H3)

## What you'll learn
- Why naive point-in-polygon is too slow at scale, and how **H3 hexagons** fix it.
- How to do **stateful** stream processing (speed from consecutive pings) in a plain Go
  consumer.
- Why we alert on **transitions** (entry / start-speeding) instead of every ping.
- How the same job would look in Spark (the spec's original choice).

## The problem

For each incoming ping the processor must answer, fast:

1. **Is this vehicle inside a geofence?** (and which one, with what speed limit)
2. **Is it speeding?** — which needs the *previous* ping for the same vehicle.

Then write the enriched event to the database, raise alerts, and push the position to the
live map. At thousands of pings/sec, step 1 is the dangerous one.

## Geofencing: from O(n) to O(1) with H3

### Why not just "point in polygon"?

The classic test (ray casting) walks every edge of a polygon per point: **O(edges)** per
ping, per polygon. With many zones and millions of pings that's a lot of trig. PostGIS with a
GIST index is smart, but a DB round-trip per ping is still far too slow for the hot path.

### The H3 idea

[H3](https://h3geo.org) is Uber's global grid that tiles the Earth in **hexagons** at 16
resolutions. Every lat/lng maps to exactly one hexagon (a 64-bit `Cell` id) at a given
resolution, in **O(1)**. The trick:

1. **Precompute (once):** "fill" each geofence polygon with the set of H3 cells that cover it
   (`PolygonToCells`). Store a map `cell → [zone ids]`.
2. **Hot path (per ping):** convert the ping's lat/lng to its H3 cell
   (`LatLngToCell`), then look it up in the map. One hash → the zones it's in.

```go
// precompute (internal/geofence/geofence.go)
cells, _ := h3.PolygonToCells(poly, resolution) // res 9 ~= 174 m hex edge
for _, c := range cells { cellToZones[c] = append(cellToZones[c], zone.ID) }

// hot path
cell, _ := h3.LatLngToCell(h3.NewLatLng(lat, lng), res)
zones := cellToZones[cell]     // O(1)
```

Geofence checking is now **independent of polygon complexity and zone count** — it's a single
map lookup no matter how many zones exist. That's the whole differentiator of this project.

### The resolution trade-off

Resolution 9 ≈ 174 m hexagon edges. Finer resolution (higher number) → more accurate polygon
edges but more cells to store; coarser → fewer cells but blockier boundaries. `PolygonToCells`
includes cells whose center is inside the polygon, so points right on the boundary can be off
by up to a cell. For street-level geofences res 9 is a good balance; safety-critical use would
go finer and/or add a PostGIS exact check on the candidates. We keep PostGIS's GIST index in
the schema precisely so you *can* cross-check.

Why we did geofencing in Go, not Spark: [ADR-0002](decisions/0002-go-processor-vs-spark.md).

## Speed detection: stateful processing

Speed = distance ÷ time between two positions. That needs the **previous** ping for the same
vehicle — i.e. **state**.

Because ingestion partitions Kafka by `vehicle_id` ([03](03-ingestion.md)), all of a
vehicle's pings arrive at **one** consumer in order. So the processor can keep a simple
in-memory map `vehicle_id → last position`:

```go
distKm := geo.HaversineKm(st.lastLat, st.lastLng, lat, lng)
dt := t.Sub(st.lastTime).Seconds()
calcSpeed := geo.SpeedKmh(distKm, dt)   // km/h
if calcSpeed > applicableLimit { /* speeding */ }
```

`applicableLimit` is the tightest speed limit among the zones the vehicle is currently in,
falling back to a default (120 km/h). We compute speed from **positions** (the spec's
approach) rather than trusting the device's reported speed, because recomputing catches
spoofed or faulty sensors.

> Spark calls this a *stateful operator* and manages the state store for you across a cluster.
> Doing it with a Go map makes the concept explicit — and it's correct here precisely because
> partitioning pins each vehicle to one consumer. If you ran multiple processor replicas,
> Kafka's consumer group would still give each vehicle to exactly one of them.

## Alert on transitions, not every ping

A truck parked in a restricted zone would otherwise emit an alert every second. So we alert
on **state changes**:

- **Geofence:** alert when a vehicle *enters* a restricted zone (was-outside → now-inside),
  tracked per zone id in `state.inRestricted`.
- **Speed:** alert when a vehicle *crosses* from under the limit to over it, tracked with
  `state.wasSpeeding`.

This is a general pattern — alert on the **edge**, not the **level** — that keeps alert
volume proportional to *events* rather than to *traffic*.

## Putting it together

Per ping, `handle` ([`internal/processor/processor.go`](../internal/processor/processor.go)):

1. H3 geofence lookup → status (`clear`/`violation`) + applicable speed limit; geofence alert
   on entry.
2. Speed calc from previous ping → speed alert on transition.
3. Update per-vehicle state.
4. Publish a `live.Position` to Redis (fire-and-forget — a slow Redis must never stall
   processing).
5. Return a `store.Event` that the run loop **batches** and writes to TimescaleDB (same
   "flush at N or T" pattern as ingestion, applied to writes).

The DB and Redis are hidden behind a small `EventStore` interface, so the whole enrichment
path is unit-tested with a fake store and no infrastructure — see
[`processor_test.go`](../internal/processor/processor_test.go), which verifies "alert on
entry only" and "alert on speeding transition only."

## The Spark reference (for comparison)

The spec's geofencing/speed logic in PySpark Structured Streaming would read the same Kafka
topic and look like this:

```python
import h3
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Broadcast the precomputed geofence cells to every executor.
geofence_cells = spark.sparkContext.broadcast(precomputed_cell_set)

@F.udf("boolean")
def in_geofence(lat, lng):
    return h3.latlng_to_cell(lat, lng, 9) in geofence_cells.value

w = Window.partitionBy("vehicle_id").orderBy("timestamp")
speed = (pings
    .withColumn("prev_lat", F.lag("latitude").over(w))
    .withColumn("prev_lng", F.lag("longitude").over(w))
    .withColumn("prev_ts",  F.lag("timestamp").over(w))
    .withColumn("calc_kmh", haversine_udf(...) / (F.col("timestamp") - F.col("prev_ts")) * 3600)
    .filter(F.col("calc_kmh") > 120))
```

Same H3 lookup, same "compare consecutive pings per vehicle" (Spark's `lag` over a
partitioned window = our per-vehicle `last position`). The Go version trades Spark's
distributed machinery for a single testable process — the reasoning is in the ADR.
