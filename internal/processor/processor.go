// Package processor consumes GPS pings from Kafka, enriches each with geofence
// status and a recomputed speed, persists them to TimescaleDB, raises alerts,
// and publishes live positions to Redis. See docs/04-processing-geospatial.md.
package processor

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	"github.com/1morshed1/fleet-telemetry/internal/geo"
	"github.com/1morshed1/fleet-telemetry/internal/geofence"
	"github.com/1morshed1/fleet-telemetry/internal/live"
	"github.com/1morshed1/fleet-telemetry/internal/store"
	fleetv1 "github.com/1morshed1/fleet-telemetry/proto/fleet/v1"
)

var (
	pingsConsumed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "processor_pings_consumed_total", Help: "Pings consumed from Kafka.",
	})
	eventsWritten = promauto.NewCounter(prometheus.CounterOpts{
		Name: "processor_events_written_total", Help: "gps_events rows written.",
	})
	speedAlerts = promauto.NewCounter(prometheus.CounterOpts{
		Name: "processor_speed_alerts_total", Help: "Speeding alerts raised.",
	})
	geofenceAlerts = promauto.NewCounter(prometheus.CounterOpts{
		Name: "processor_geofence_alerts_total", Help: "Geofence violation alerts raised.",
	})
	dbErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "processor_db_errors_total", Help: "Database write errors.",
	})
	liveErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "processor_live_publish_errors_total", Help: "Redis publish errors (live positions).",
	})
	liveDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "processor_live_dropped_total", Help: "Live positions dropped due to a full publish buffer.",
	})
)

// vehicleState is the per-vehicle memory needed for stateful processing: the
// previous position (for speed) and which zones/speeding state it was last in
// (so we alert on transitions, not on every ping).
type vehicleState struct {
	lastLat, lastLng float64
	lastTime         time.Time
	hasPrev          bool
	inRestricted     map[int]bool // zone IDs currently violated
	wasSpeeding      bool
}

// Config tunes the processor.
type Config struct {
	DefaultSpeedLimit float64       // used when a ping is outside any speed-limited zone
	FlushSize         int           // flush events to DB at this many
	FlushInterval     time.Duration // ...or at least this often
}

// DefaultConfig is a reasonable local default.
func DefaultConfig() Config {
	return Config{DefaultSpeedLimit: 120, FlushSize: 500, FlushInterval: time.Second}
}

// EventStore is the subset of the DB the processor needs. Narrowing to an
// interface lets tests substitute a fake store (no database required).
type EventStore interface {
	InsertEvents(ctx context.Context, evs []store.Event) error
	InsertSpeedAlert(ctx context.Context, t time.Time, vehicleID string, speed, limit, lat, lng float64) error
	InsertGeofenceAlert(ctx context.Context, t time.Time, vehicleID string, zoneID int, zoneName string, lat, lng float64) error
}

// Processor ties together Kafka input, geofence index, DB, and Redis output.
type Processor struct {
	reader *kafka.Reader
	store  EventStore
	geo    *geofence.Index
	rdb    *redis.Client
	cfg    Config

	state  map[string]*vehicleState
	liveCh chan live.Position // decouples Redis publishing from the hot path
}

// New builds a processor. The geofence index is loaded once by the caller and
// passed in (it can be refreshed by rebuilding and swapping).
func New(reader *kafka.Reader, st EventStore, idx *geofence.Index, rdb *redis.Client, cfg Config) *Processor {
	return &Processor{
		reader: reader, store: st, geo: idx, rdb: rdb, cfg: cfg,
		state:  make(map[string]*vehicleState),
		liveCh: make(chan live.Position, 10_000),
	}
}

// Run consumes until ctx is cancelled. A reader goroutine decodes Kafka messages
// into pings; the main loop enriches them, batches DB writes, and flushes on
// size or interval — the same batching pattern as ingestion, applied to writes.
func (p *Processor) Run(ctx context.Context) error {
	pingCh := make(chan *fleetv1.GPSPing, 10_000)

	// Publisher goroutine: drains liveCh and pushes to Redis with a bounded
	// timeout. Kept OFF the hot path so a slow/absent Redis can never stall
	// enrichment or DB writes (handle() only does a non-blocking channel send).
	go p.runPublisher(ctx)

	// Reader goroutine.
	go func() {
		defer close(pingCh)
		for {
			m, err := p.reader.ReadMessage(ctx)
			if err != nil {
				return // ctx cancelled or reader closed
			}
			var ping fleetv1.GPSPing
			if err := proto.Unmarshal(m.Value, &ping); err != nil {
				log.Printf("processor: bad protobuf: %v", err)
				continue
			}
			pingsConsumed.Inc()
			select {
			case pingCh <- &ping:
			case <-ctx.Done():
				return
			}
		}
	}()

	ticker := time.NewTicker(p.cfg.FlushInterval)
	defer ticker.Stop()
	batch := make([]store.Event, 0, p.cfg.FlushSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := p.store.InsertEvents(ctx, batch); err != nil {
			dbErrors.Inc()
			log.Printf("processor: insert events: %v", err)
		} else {
			eventsWritten.Add(float64(len(batch)))
		}
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return ctx.Err()
		case ping, ok := <-pingCh:
			if !ok {
				flush()
				return nil
			}
			ev := p.handle(ctx, ping)
			batch = append(batch, ev)
			if len(batch) >= p.cfg.FlushSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// handle enriches one ping: geofence check, speed calc, alerts, live publish.
// It returns the Event to persist.
func (p *Processor) handle(ctx context.Context, ping *fleetv1.GPSPing) store.Event {
	vid := ping.GetVehicleId()
	lat, lng := ping.GetLatitude(), ping.GetLongitude()
	t := time.UnixMilli(ping.GetTimestampUnixMs())

	st := p.state[vid]
	if st == nil {
		st = &vehicleState{inRestricted: map[int]bool{}}
		p.state[vid] = st
	}

	// --- Geofencing (O(1) via H3) ---
	zones, err := p.geo.Lookup(lat, lng)
	if err != nil {
		log.Printf("processor: geofence lookup: %v", err)
	}
	status := "clear"
	applicableLimit := p.cfg.DefaultSpeedLimit
	currentRestricted := map[int]bool{}
	for _, z := range zones {
		if z.SpeedLimit > 0 && z.SpeedLimit < applicableLimit {
			applicableLimit = z.SpeedLimit
		}
		if z.Type == "restricted" {
			status = "violation"
			currentRestricted[z.ID] = true
			// Alert only on ENTRY (transition), not every ping inside.
			if !st.inRestricted[z.ID] {
				geofenceAlerts.Inc()
				if err := p.store.InsertGeofenceAlert(ctx, t, vid, z.ID, z.Name, lat, lng); err != nil {
					dbErrors.Inc()
				}
			}
		}
	}
	st.inRestricted = currentRestricted

	// --- Speed detection (stateful, from consecutive pings) ---
	calcSpeed := ping.GetSpeedKmh() // fall back to reported speed if no previous
	if st.hasPrev {
		distKm := geo.HaversineKm(st.lastLat, st.lastLng, lat, lng)
		dt := t.Sub(st.lastTime).Seconds()
		if dt > 0 {
			calcSpeed = geo.SpeedKmh(distKm, dt)
		}
	}
	if calcSpeed > applicableLimit {
		if !st.wasSpeeding { // alert on transition into speeding
			speedAlerts.Inc()
			if err := p.store.InsertSpeedAlert(ctx, t, vid, calcSpeed, applicableLimit, lat, lng); err != nil {
				dbErrors.Inc()
			}
		}
		st.wasSpeeding = true
	} else {
		st.wasSpeeding = false
	}

	st.lastLat, st.lastLng, st.lastTime, st.hasPrev = lat, lng, t, true

	// --- Publish live position (fire-and-forget, off the hot path) ---
	p.publishLive(live.Position{
		VehicleID: vid, Lat: lat, Lng: lng,
		SpeedKmh: calcSpeed, Heading: ping.GetHeading(),
		Status: status, TimeMs: ping.GetTimestampUnixMs(),
	})

	return store.Event{
		Time: t, VehicleID: vid, Lat: lat, Lng: lng,
		SpeedKmh: calcSpeed, Heading: ping.GetHeading(), GeofenceStatus: status,
	}
}

// publishLive hands a position to the publisher goroutine without blocking. If
// the buffer is full (Redis is slow/down), the update is dropped — live data is
// best-effort; the durable copy is already in TimescaleDB.
func (p *Processor) publishLive(pos live.Position) {
	if p.rdb == nil {
		return
	}
	select {
	case p.liveCh <- pos:
	default:
		liveDropped.Inc()
	}
}

// runPublisher drains liveCh and publishes to Redis, bounding each publish so a
// broker problem cannot wedge the goroutine.
func (p *Processor) runPublisher(ctx context.Context) {
	if p.rdb == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case pos := <-p.liveCh:
			b, err := json.Marshal(pos)
			if err != nil {
				continue
			}
			pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			if err := p.rdb.Publish(pctx, live.Channel, b).Err(); err != nil {
				liveErrors.Inc()
			}
			cancel()
		}
	}
}
