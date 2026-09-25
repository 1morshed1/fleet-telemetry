## Project 3: Real-Time IoT Fleet Management & Geospatial Dashboard

### Concept & Why It Matters

You are building a system that ingests millions of GPS pings per second from a simulated fleet of vehicles, processes them in real-time to detect geofence violations and speeding, stores the data in a time-series database, and displays everything on a live dashboard with a map.

This project has the highest data ingestion volume of all three. It demonstrates your ability to handle extreme throughput at the ingestion layer (Go + gRPC streaming), buffer it reliably (Kafka), process it with complex geospatial logic (Spark), and store it efficiently (TimescaleDB). The geospatial angle also differentiates it from generic data pipeline projects.

### Architecture Diagram

```
  Simulated IoT Devices                                                         
  (Go program generating                                                        
   GPS pings)                                                                   
       │                                                                        
       │  gRPC bidirectional streaming                                          
       │  (millions of pings/sec)                                               
       ▼                                                                        
┌──────────────────────────────────────────────────────────────────────────────┐
│                        KUBERNETES CLUSTER (ArgoCD-managed)                    │
│                                                                              │
│  ┌────────────────┐                                                          │
│  │  Go Ingestion  │──── Prometheus metrics (pings/sec, latency)              │
│  │  Service       │                                                          │
│  │  (gRPC server) │                                                          │
│  │  Horizontally  │                                                          │
│  │  scaled (HPA)  │                                                          │
│  └───────┬────────┘                                                          │
│          │                                                                   │
│          │  Produce to Kafka                                                 │
│          ▼                                                                   │
│  ┌──────────────────────────────────────────────────────┐                     │
│  │  Apache Kafka                                        │                    │
│  │  topic: gps-pings        (partitioned by vehicle_id) │                    │
│  │  topic: geofence-alerts  (violations)                │                    │
│  │  topic: speed-alerts     (speeding events)           │                    │
│  └───────────────────┬──────────────────────────────────┘                     │
│                      │                                                       │
│            ┌─────────┴──────────┐                                            │
│            ▼                    ▼                                            │
│  ┌──────────────────┐  ┌──────────────────┐                                  │
│  │  Spark Streaming  │  │  Spark Streaming  │                                │
│  │  Job: Geofencing  │  │  Job: Speed       │                                │
│  │                   │  │  Detection        │                                │
│  │  Uses PostGIS /   │  │                   │                                │
│  │  H3 hex indexing  │  │  Sliding window   │                                │
│  │  for point-in-    │  │  speed calc       │                                │
│  │  polygon checks   │  │                   │                                │
│  └────────┬──────────┘  └────────┬──────────┘                                │
│           │                      │                                           │
│           ▼                      ▼                                           │
│  ┌──────────────────────────────────────────┐                                │
│  │  TimescaleDB (Postgres + Timescale ext)  │                                │
│  │                                          │                                │
│  │  Hypertable: gps_events                  │                                │
│  │  (auto-partitioned by time)              │                                │
│  │                                          │                                │
│  │  Table: geofence_zones                   │                                │
│  │  (PostGIS geometry column)               │                                │
│  │                                          │                                │
│  │  Continuous Aggregate: hourly_summaries  │                                │
│  └──────────────────────────────────────────┘                                │
│                                                                              │
│  ┌──────────────────────────────────────────┐                                │
│  │  Go Dashboard API                        │                                │
│  │  REST: historical queries                │                                │
│  │  WebSocket: live vehicle positions       │                                │
│  └──────────────────────────────────────────┘                                │
│                                                                              │
│  ┌──────────────────────────────────────────┐                                │
│  │  Observability: Prometheus + Grafana     │                                │
│  │  + Jaeger + Loki                         │                                │
│  │                                          │                                │
│  │  ArgoCD: GitOps continuous deployment    │                                │
│  └──────────────────────────────────────────┘                                │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Tech Stack Breakdown

| Layer | Technology | Why This Choice |
|---|---|---|
| Ingestion | Go gRPC (bidirectional streaming) | gRPC streaming lets you maintain a persistent connection and send thousands of pings per second over a single TCP connection. Far more efficient than individual HTTP requests. |
| Buffering | Apache Kafka | Same reasons as Project 1. Partition by `vehicle_id` to ensure all pings from the same vehicle are ordered. |
| Stream Processing | Apache Spark Structured Streaming | Windowed aggregations for speed calculation. Stateful processing for geofencing. |
| Geospatial | PostGIS + H3 (Uber's hexagonal indexing) | PostGIS adds geometry types and spatial queries to Postgres. H3 converts lat/lng into hexagonal grid cells, making "is this point inside this polygon" checks extremely fast at scale. |
| Time-Series Storage | TimescaleDB | Postgres extension optimized for time-series data. Hypertables auto-partition by time. Continuous aggregates precompute hourly/daily summaries. Compression reduces storage by 90%+. |
| Live Dashboard | Go + WebSocket | The dashboard API pushes live vehicle positions to a frontend via WebSocket. Historical queries use standard REST. |
| GitOps | ArgoCD | Watches a Git repository containing K8s manifests. Any push to the repo triggers an automatic cluster update. This replaces manual `kubectl apply`. |
| Cluster Config | Ansible | For bootstrapping the K8s nodes themselves (installing container runtime, configuring kubelet). Terraform provisions the VMs; Ansible configures them. |

### Phase-by-Phase Build Plan

#### Phase 1: The Simulator (Go)

Before you build the ingestion service, you need something to ingest. Build a GPS simulator:

```go
// Each simulated vehicle runs as a goroutine
type Vehicle struct {
    ID        string
    Lat       float64
    Lng       float64
    Speed     float64  // km/h
    Heading   float64  // degrees
    RouteType string   // "highway", "urban", "rural"
}

func (v *Vehicle) Tick(interval time.Duration) GPSPing {
    // Update position based on speed and heading
    // Add realistic noise (GPS jitter: ±5 meters)
    // Occasionally change heading (simulate turns)
    // Vary speed based on route type
    distanceKm := v.Speed * interval.Hours()
    v.Lat += distanceKm / 111.0 * math.Cos(v.Heading*math.Pi/180)
    v.Lng += distanceKm / (111.0 * math.Cos(v.Lat*math.Pi/180)) * math.Sin(v.Heading*math.Pi/180)

    // Add GPS jitter
    v.Lat += (rand.Float64() - 0.5) * 0.0001
    v.Lng += (rand.Float64() - 0.5) * 0.0001

    return GPSPing{
        VehicleID: v.ID,
        Lat:       v.Lat,
        Lng:       v.Lng,
        Speed:     v.Speed + (rand.Float64()-0.5)*10, // ±5 km/h noise
        Heading:   v.Heading,
        Timestamp: time.Now(),
    }
}
```

Run 10,000 simulated vehicles, each emitting a ping every second. That is 10,000 pings/sec sustained — enough to stress-test your pipeline.

#### Phase 2: The Ingestion Service (Go gRPC Streaming)

**Protobuf Definition:**

```protobuf
syntax = "proto3";
package fleet;

service FleetIngestion {
    // Bidirectional streaming: devices send pings, server sends acks
    rpc StreamPings(stream GPSPing) returns (stream PingAck);
}

message GPSPing {
    string vehicle_id = 1;
    double latitude = 2;
    double longitude = 3;
    double speed_kmh = 4;
    double heading = 5;
    int64 timestamp_unix_ms = 6;
}

message PingAck {
    string vehicle_id = 1;
    bool accepted = 2;
    string reason = 3;  // rejection reason if not accepted
}
```

**Performance Optimizations:**

- **Batch Kafka writes:** Do not produce one Kafka message per ping. Buffer pings in memory (e.g., 500 pings or 100ms, whichever comes first) and produce them as a batch. This dramatically reduces Kafka overhead.
- **Protobuf for Kafka values:** Serialize the Kafka message value as Protobuf, not JSON. Protobuf is 3-5x smaller and 10x faster to serialize/deserialize.
- **Connection pooling:** Use a pool of Kafka producers (one per CPU core) to avoid contention.
- **Backpressure handling:** If Kafka is slow, the gRPC server should start sending `accepted: false` in `PingAck` rather than buffering indefinitely and running out of memory.

#### Phase 3: Geospatial Processing (Spark + PostGIS + H3)

This is where the project differentiates itself from generic Kafka + Spark pipelines.

**Geofencing with H3 Hexagonal Indexing:**

Traditional point-in-polygon checks are O(n) per polygon edge. For millions of pings per second, this is too slow. H3 solves this:

1. **Precompute:** Convert each geofence polygon into a set of H3 hex cells at resolution 9 (about 100m per cell). Store this set in a broadcast variable in Spark.
2. **At processing time:** Convert each GPS ping's lat/lng into an H3 cell index (a single O(1) operation). Check if this cell exists in the geofence's set of cells (O(1) hash lookup).
3. **Result:** Geofence checking becomes O(1) per ping instead of O(n).

```python
import h3

# Precompute: convert geofence polygon to H3 cells
def polygon_to_h3_cells(polygon_coords, resolution=9):
    return h3.polyfill_geojson(
        {"type": "Polygon", "coordinates": [polygon_coords]},
        resolution
    )

# At processing time: check if a ping is inside a geofence
def check_geofence(lat, lng, geofence_cells, resolution=9):
    ping_cell = h3.latlng_to_cell(lat, lng, resolution)
    return ping_cell in geofence_cells
```

**Speed Detection with Sliding Windows:**

Calculate speed between consecutive pings for the same vehicle. Flag if the calculated speed exceeds the road's speed limit:

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

vehicle_window = Window.partitionBy("vehicle_id").orderBy("timestamp")

speed_df = (
    pings_df
    .withColumn("prev_lat", F.lag("latitude").over(vehicle_window))
    .withColumn("prev_lng", F.lag("longitude").over(vehicle_window))
    .withColumn("prev_ts", F.lag("timestamp").over(vehicle_window))
    .withColumn("distance_km", haversine_udf("prev_lat", "prev_lng", "latitude", "longitude"))
    .withColumn("time_hours", (F.col("timestamp").cast("long") - F.col("prev_ts").cast("long")) / 3600)
    .withColumn("calculated_speed_kmh", F.col("distance_km") / F.col("time_hours"))
    .filter(F.col("calculated_speed_kmh") > F.lit(120))  # Flag speeds over 120 km/h
)
```

#### Phase 4: TimescaleDB Storage

**Schema:**

```sql
-- Enable extensions
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS postgis;

-- Main hypertable for GPS events
CREATE TABLE gps_events (
    time            TIMESTAMPTZ NOT NULL,
    vehicle_id      TEXT NOT NULL,
    location        GEOMETRY(Point, 4326) NOT NULL,
    speed_kmh       DOUBLE PRECISION,
    heading         DOUBLE PRECISION,
    geofence_status TEXT DEFAULT 'clear'  -- 'clear', 'violation'
);

SELECT create_hypertable('gps_events', 'time');

-- Continuous aggregate for dashboard queries
CREATE MATERIALIZED VIEW hourly_vehicle_summary
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    vehicle_id,
    AVG(speed_kmh) AS avg_speed,
    MAX(speed_kmh) AS max_speed,
    COUNT(*) AS ping_count,
    ST_MakeLine(location ORDER BY time) AS route_line  -- constructs the route geometry
FROM gps_events
GROUP BY time_bucket('1 hour', time), vehicle_id;

-- Compression policy: compress data older than 7 days
SELECT add_compression_policy('gps_events', INTERVAL '7 days');

-- Retention policy: drop data older than 90 days
SELECT add_retention_policy('gps_events', INTERVAL '90 days');

-- Geofence zones table
CREATE TABLE geofence_zones (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    zone_type   TEXT NOT NULL,       -- 'restricted', 'warehouse', 'delivery_area'
    boundary    GEOMETRY(Polygon, 4326) NOT NULL,
    speed_limit DOUBLE PRECISION     -- km/h, NULL if no limit
);

CREATE INDEX idx_geofence_boundary ON geofence_zones USING GIST(boundary);
```

TimescaleDB's hypertables automatically partition the `gps_events` table by time (default: 7-day chunks). This means queries like "show me vehicle X's route for the last hour" only scan one chunk instead of the entire table. The continuous aggregate precomputes hourly summaries so dashboard queries are instant.

#### Phase 5: The Dashboard API (Go + WebSocket)

**REST Endpoints (historical queries):**

- `GET /api/v1/vehicles` — List all vehicles with their last known position.
- `GET /api/v1/vehicles/:id/history?from=...&to=...` — Return the route (as GeoJSON LineString) and speed data for a vehicle over a time range.
- `GET /api/v1/geofences` — List all geofence zones as GeoJSON polygons.
- `GET /api/v1/alerts?type=speed&from=...&to=...` — List all speed or geofence alerts.

**WebSocket Endpoint (live updates):**

- `WS /api/v1/live` — Subscribes the client to real-time vehicle position updates. The Go server consumes from a Redis Pub/Sub channel (populated by the ingestion service) and pushes updates to all connected WebSocket clients. Send updates at most once per second per vehicle to avoid overwhelming the frontend.

#### Phase 6: GitOps with ArgoCD

Instead of deploying with `kubectl apply` in a CI pipeline, you use GitOps:

**Repository Structure:**

```
fleet-platform-gitops/
├── base/
│   ├── ingestion-service/
│   │   ├── deployment.yaml
│   │   ├── service.yaml
│   │   └── hpa.yaml
│   ├── spark-jobs/
│   │   ├── geofence-job.yaml
│   │   └── speed-detection-job.yaml
│   ├── timescaledb/
│   │   ├── statefulset.yaml
│   │   ├── service.yaml
│   │   └── pvc.yaml
│   ├── dashboard-api/
│   │   ├── deployment.yaml
│   │   ├── service.yaml
│   │   └── ingress.yaml
│   ├── kafka/
│   │   └── strimzi-kafka-cluster.yaml
│   └── monitoring/
│       ├── prometheus-values.yaml
│       └── grafana-dashboards/
├── overlays/
│   ├── dev/
│   │   └── kustomization.yaml    # Lower replicas, smaller resource limits
│   └── prod/
│       └── kustomization.yaml    # Higher replicas, proper resource limits
└── argocd/
    └── application.yaml          # ArgoCD Application manifest
```

**How it works:**
1. Your CI pipeline (GitHub Actions) builds the Docker image, pushes to ECR, and updates the image tag in the GitOps repo (using `kustomize edit set image`).
2. ArgoCD detects the change in the GitOps repo and automatically syncs the cluster to match.
3. If a deployment fails (pods crash-looping), ArgoCD shows the diff and you can roll back by reverting the Git commit.

This is how teams at Google, Spotify, and Intuit manage production Kubernetes clusters.

### Observability Strategy

Same three pillars as Projects 1 and 2, plus:

- **Ingestion throughput dashboard:** A real-time Grafana panel showing pings/second being ingested, partitioned by vehicle type. This is the headline metric for this project.
- **Kafka consumer lag monitoring:** Critical for this project because the data volume is so high. If Spark falls behind, the lag will grow. Set up PagerDuty-style alerts if lag exceeds 10,000 messages.
- **Geospatial visualization in Grafana:** Use the Grafana GeoMap panel to display vehicle positions and geofence zones on a real map. This is visually impressive in demos.

### Load Testing & Benchmarking

The simulator IS your load test. Document:
- How many vehicles can your system handle before Kafka lag starts growing? (Target: 50,000+ vehicles at 1 ping/sec)
- What is the end-to-end latency from ping emission to geofence alert? (Target: under 5 seconds)
- How does TimescaleDB query performance degrade over time? Run the same query against 1 day, 1 week, and 1 month of data.

### Cost Management

- Run TimescaleDB, Kafka (via Strimzi operator), and Spark locally using Docker Compose for development.
- Use Minikube for ArgoCD testing.
- On AWS, TimescaleDB on a `db.t3.medium` RDS instance with the Timescale extension is about $30/month.
- Use Kafka's `log.retention.hours=72` to limit disk usage.

---
---