// Package ingestion implements the gRPC front door. It accepts bidirectional
// streams of GPS pings, applies backpressure when overloaded, batches accepted
// pings, and produces them to Kafka keyed by vehicle_id.
package ingestion

import (
	"context"
	"log"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	fleetv1 "github.com/1morshed1/fleet-telemetry/proto/fleet/v1"
)

// Metrics for the ingestion service. Registered on the default registry so the
// /metrics endpoint exposes them.
var (
	pingsReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ingestion_pings_received_total",
		Help: "Total GPS pings received over gRPC.",
	})
	pingsRejected = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ingestion_pings_rejected_total",
		Help: "Pings rejected due to a full ingest queue (backpressure).",
	})
	pingsProduced = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ingestion_pings_produced_total",
		Help: "Pings successfully produced to Kafka.",
	})
	batchesFlushed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ingestion_batches_flushed_total",
		Help: "Number of Kafka write batches flushed.",
	})
	batchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ingestion_batch_size",
		Help:    "Number of pings per flushed batch.",
		Buckets: []float64{1, 10, 50, 100, 200, 500, 1000},
	})
	kafkaErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ingestion_kafka_write_errors_total",
		Help: "Failed Kafka batch writes.",
	})
	queueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ingestion_queue_depth",
		Help: "Current number of pings buffered awaiting batch flush.",
	})
)

// writer is the subset of *kafka.Writer we use, so tests can substitute a fake.
type writer interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

// Config tunes the batcher and queue.
type Config struct {
	QueueSize     int           // max buffered pings before backpressure kicks in
	MaxBatch      int           // flush when this many pings accumulate
	FlushInterval time.Duration // flush at least this often
}

// DefaultConfig matches the plan: 500 pings or 100ms, whichever comes first.
func DefaultConfig() Config {
	return Config{QueueSize: 100_000, MaxBatch: 500, FlushInterval: 100 * time.Millisecond}
}

// Server implements fleetv1.FleetIngestionServer.
type Server struct {
	fleetv1.UnimplementedFleetIngestionServer

	w     writer
	queue chan *fleetv1.GPSPing
	cfg   Config
}

// NewServer creates the server and starts its background batcher goroutine.
// The batcher stops when ctx is cancelled.
func NewServer(ctx context.Context, w writer, cfg Config) *Server {
	s := &Server{
		w:     w,
		queue: make(chan *fleetv1.GPSPing, cfg.QueueSize),
		cfg:   cfg,
	}
	go s.runBatcher(ctx)
	return s
}

// StreamPings is the bidirectional RPC. For every ping received it tries to
// enqueue it; if the queue is full it replies accepted=false so the client can
// slow down (backpressure) instead of the server buffering to OOM.
func (s *Server) StreamPings(stream fleetv1.FleetIngestion_StreamPingsServer) error {
	for {
		ping, err := stream.Recv()
		if err != nil {
			return err // io.EOF on clean client close, or a real error
		}
		pingsReceived.Inc()

		accepted := s.tryEnqueue(ping)
		if accepted {
			queueDepth.Set(float64(len(s.queue)))
		} else {
			pingsRejected.Inc()
		}

		ack := &fleetv1.PingAck{VehicleId: ping.GetVehicleId(), Accepted: accepted}
		if !accepted {
			ack.Reason = "ingest queue full"
		}
		if err := stream.Send(ack); err != nil {
			return err
		}
	}
}

// tryEnqueue does a non-blocking send: enqueue if there's room, otherwise report
// the queue as full. This is the backpressure primitive.
func (s *Server) tryEnqueue(ping *fleetv1.GPSPing) bool {
	select {
	case s.queue <- ping:
		return true
	default:
		return false
	}
}

// runBatcher drains the queue, accumulating pings into a batch that is flushed to
// Kafka when it reaches MaxBatch or when FlushInterval elapses — whichever first.
// Batching amortizes the per-request Kafka overhead across many pings.
func (s *Server) runBatcher(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.FlushInterval)
	defer ticker.Stop()

	batch := make([]*fleetv1.GPSPing, 0, s.cfg.MaxBatch)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		s.flush(ctx, batch)
		batch = batch[:0] // reuse the backing array
		queueDepth.Set(float64(len(s.queue)))
	}

	for {
		select {
		case <-ctx.Done():
			flush() // best-effort final flush on shutdown
			return
		case ping := <-s.queue:
			batch = append(batch, ping)
			if len(batch) >= s.cfg.MaxBatch {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// flush serializes a batch to protobuf Kafka messages (keyed by vehicle_id) and
// writes them in one call.
func (s *Server) flush(ctx context.Context, batch []*fleetv1.GPSPing) {
	msgs := make([]kafka.Message, 0, len(batch))
	for _, p := range batch {
		val, err := proto.Marshal(p)
		if err != nil {
			log.Printf("ingestion: marshal ping: %v", err)
			continue
		}
		msgs = append(msgs, kafka.Message{Key: []byte(p.GetVehicleId()), Value: val})
	}
	if len(msgs) == 0 {
		return
	}

	// Bound the write so a stalled broker can't wedge the batcher forever.
	wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := s.w.WriteMessages(wctx, msgs...); err != nil {
		kafkaErrors.Inc()
		log.Printf("ingestion: kafka write (%d msgs): %v", len(msgs), err)
		return
	}
	pingsProduced.Add(float64(len(msgs)))
	batchesFlushed.Inc()
	batchSize.Observe(float64(len(msgs)))
}
