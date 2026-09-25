package ingestion

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	fleetv1 "github.com/1morshed1/fleet-telemetry/proto/fleet/v1"
)

// fakeWriter records the messages it is asked to write.
type fakeWriter struct {
	mu   sync.Mutex
	msgs []kafka.Message
}

func (f *fakeWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.msgs = append(f.msgs, msgs...)
	return nil
}

func (f *fakeWriter) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.msgs)
}

func (f *fakeWriter) all() []kafka.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]kafka.Message(nil), f.msgs...)
}

func TestBatcherFlushesByInterval(t *testing.T) {
	fw := &fakeWriter{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := NewServer(ctx, fw, Config{QueueSize: 100, MaxBatch: 500, FlushInterval: 20 * time.Millisecond})

	// Fewer than MaxBatch pings: they should flush on the interval, not by size.
	for i := 0; i < 3; i++ {
		if !s.tryEnqueue(&fleetv1.GPSPing{VehicleId: "v", Latitude: float64(i)}) {
			t.Fatal("enqueue unexpectedly rejected")
		}
	}

	waitFor(t, func() bool { return fw.count() == 3 }, time.Second)

	// Verify the payload round-trips through protobuf and is keyed by vehicle_id.
	for _, m := range fw.all() {
		if string(m.Key) != "v" {
			t.Fatalf("message key = %q, want v", m.Key)
		}
		var p fleetv1.GPSPing
		if err := proto.Unmarshal(m.Value, &p); err != nil {
			t.Fatalf("value not valid protobuf: %v", err)
		}
	}
}

func TestBatcherFlushesBySize(t *testing.T) {
	fw := &fakeWriter{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Large interval so only the size trigger can fire within the test window.
	s := NewServer(ctx, fw, Config{QueueSize: 1000, MaxBatch: 10, FlushInterval: time.Hour})

	for i := 0; i < 10; i++ {
		s.tryEnqueue(&fleetv1.GPSPing{VehicleId: "v"})
	}
	waitFor(t, func() bool { return fw.count() == 10 }, time.Second)
}

func TestBackpressureWhenQueueFull(t *testing.T) {
	fw := &fakeWriter{}
	// No batcher draining: construct with a tiny queue and never start... but
	// NewServer starts the batcher. Use a huge interval + huge batch so nothing
	// flushes, and a queue of size 2 so the 3rd enqueue is rejected.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := NewServer(ctx, fw, Config{QueueSize: 2, MaxBatch: 1_000_000, FlushInterval: time.Hour})

	// The batcher may pull one item off the queue immediately, so fill generously
	// and assert that at least one enqueue is eventually rejected.
	rejected := false
	for i := 0; i < 50; i++ {
		if !s.tryEnqueue(&fleetv1.GPSPing{VehicleId: "v"}) {
			rejected = true
			break
		}
	}
	if !rejected {
		t.Fatal("expected backpressure rejection with a full queue")
	}
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}
