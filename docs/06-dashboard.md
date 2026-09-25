# 06 — The Dashboard API & Web

## What you'll learn
- When to use **REST** vs **WebSocket**, and why this project needs both.
- How **Redis Pub/Sub** decouples "one processor publishing" from "many browsers watching."
- The **coalescing fan-out** pattern that protects browsers from a firehose.
- Turning PostGIS geometry into **GeoJSON** a map can render directly.

## The problem

The dashboard has two very different data needs:

1. **Historical / on-request:** "list all vehicles," "vehicle 42's route for the last hour,"
   "recent alerts." These are classic request/response — a **REST** API over TimescaleDB.
2. **Live / continuous:** a map that updates as vehicles move, without the browser polling
   every second. That's a **WebSocket** — the server pushes.

Source: [`internal/dashboard/`](../internal/dashboard/), [`cmd/dashboard/main.go`](../cmd/dashboard/main.go),
front-end in [`web/`](../web/).

## REST endpoints

| Endpoint | Returns | Backed by |
|----------|---------|-----------|
| `GET /api/v1/vehicles` | latest position per vehicle (last 5 min) | `DISTINCT ON (vehicle_id) … ORDER BY vehicle_id, time DESC` |
| `GET /api/v1/vehicles/{id}/history?from=&to=` | route as GeoJSON LineString + speed points | `ST_MakeLine(location ORDER BY time)` |
| `GET /api/v1/geofences` | zones as a GeoJSON FeatureCollection | `json_build_object(...)` + `ST_AsGeoJSON` |
| `GET /api/v1/alerts?type=speed\|geofence\|all&from=&to=` | recent alerts | the alert hypertables |

Two PostGIS-to-web tricks worth calling out:

- **`DISTINCT ON`** is Postgres's clean "latest row per group." Combined with the
  `(vehicle_id, time DESC)` index it's cheap.
- The geofences endpoint builds the **entire GeoJSON FeatureCollection in SQL** with
  `json_build_object`/`json_agg`, so the handler just streams the bytes through — no Go-side
  assembly. The browser drops the result straight onto Leaflet with `L.geoJSON(fc)`.

Query code: [`internal/store/queries.go`](../internal/store/queries.go).

## Live updates: Redis Pub/Sub + WebSocket

### Why Redis in the middle?

The processor produces one stream of live positions. But there can be zero, one, or fifty
browsers connected. We don't want the processor to know or care. So the processor
**publishes** each position to a Redis channel (`live:positions`), and the dashboard
**subscribes** once and re-broadcasts to whatever browsers are attached.

```
processor ──PUBLISH──▶ Redis channel ──SUBSCRIBE──▶ dashboard Hub ──WS──▶ browsers
```

Redis Pub/Sub is fire-and-forget (no durability) — perfect for "live" data where the newest
value is all that matters and history already lives in TimescaleDB. It also means you can run
**multiple dashboard replicas**: each subscribes to the same channel and serves its own set of
browsers.

### The coalescing fan-out (the important bit)

A naive hub would forward every ping to every browser. At 10,000 vehicles that's 10,000
messages/sec **per browser** — it would melt the tab. The [`Hub`](../internal/dashboard/hub.go)
instead **coalesces**:

- It keeps only the **latest** position per vehicle (`latest[vehicle_id]`) and a `dirty` set of
  which vehicles changed.
- On a fixed **broadcast interval** (default 1 s) it sends one batch of just the changed
  positions to all clients.

So client traffic is bounded by *(vehicles that moved) per (interval)*, independent of the
ingest rate. This is the same idea as batching in ingestion ([03](03-ingestion.md)) and write
flushing in the processor ([04](04-processing-geospatial.md)) — **decouple the fast producer
from the slow consumer with a buffer + periodic flush**.

### Handling slow / dead clients

Each client has a small buffered `send` channel and its own **write pump**:

- If a client's buffer is full during broadcast, we **drop** that update for that client
  (`select { case c.send <- msg: default: }`) rather than block the whole hub for everyone.
- A **read pump** with pong deadlines detects disconnects; a **ping** every 30 s keeps
  intermediaries from closing idle connections.
- A newly connected client immediately gets a **snapshot** of all known positions, so the map
  populates instantly instead of filling in as vehicles happen to move.

These are the standard gorilla/websocket hub patterns; the comments in `hub.go` map each part
to its purpose.

## The web page

[`web/index.html`](../web/index.html) + [`web/app.js`](../web/app.js): a Leaflet map with a
dark CARTO basemap.

- On load: `GET /api/v1/geofences` → draws polygons colored by type (restricted = red,
  warehouse = amber, delivery = green).
- Opens the WebSocket, and for each position in a batch **upserts** a circle marker (blue =
  clear, red = in violation), updating its position and tooltip.
- Polls `/api/v1/alerts` every 5 s for the side feed and the stat tiles.
- Auto-reconnects the WebSocket with a 2 s backoff if it drops.

No build step, no framework — plain JS and a CDN Leaflet, served as static files by the Go
dashboard so the whole thing is one container.

## Try it

With the stack up and the simulator running:

```
http://localhost:8080
```

You should see markers moving, geofence polygons, some turning red as they enter the restricted
zone, and speed/geofence alerts streaming into the side panel.
