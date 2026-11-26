// Command simulator stands in for a fleet of GPS devices. It creates N
// simulated vehicles and streams their pings to the ingestion service over a
// single gRPC bidirectional stream, reading back acks to observe backpressure.
//
// Usage:
//
//	go run ./cmd/simulator -vehicles=1000 -addr=localhost:50051
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/1morshed1/fleet-telemetry/internal/simulator"
	fleetv1 "github.com/1morshed1/fleet-telemetry/proto/fleet/v1"
)

func main() {
	var (
		addr      = flag.String("addr", "localhost:50051", "ingestion gRPC address")
		vehicles  = flag.Int("vehicles", 1000, "number of simulated vehicles")
		interval  = flag.Duration("interval", time.Second, "ping interval per vehicle")
		centerLat = flag.Float64("center-lat", 52.52, "fleet spawn center latitude")
		centerLng = flag.Float64("center-lng", 13.405, "fleet spawn center longitude")
		radiusKm  = flag.Float64("radius", 15.0, "fleet spawn radius in km")
		seed      = flag.Int64("seed", 1, "base RNG seed (reproducible fleets)")
	)
	flag.Parse()

	// Build the fleet. Each vehicle gets a distinct seed derived from the base.
	fleet := make([]*simulator.Vehicle, *vehicles)
	for i := range fleet {
		id := "veh-" + pad(i)
		fleet[i] = simulator.NewVehicle(id, *centerLat, *centerLng, *radiusKm, *seed+int64(i))
	}
	log.Printf("simulator: created %d vehicles around (%.4f, %.4f)", len(fleet), *centerLat, *centerLng)

	// Connect to ingestion. insecure is fine for local/dev; production would use TLS.
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("simulator: dial %s: %v", *addr, err)
	}
	defer conn.Close()
	client := fleetv1.NewFleetIngestionClient(conn)

	ctx, cancel := signalContext()
	defer cancel()

	stream, err := client.StreamPings(ctx)
	if err != nil {
		log.Fatalf("simulator: open stream: %v", err)
	}

	var sent, accepted, rejected atomic.Int64

	// Reader goroutine: consume acks so the server's send buffer never blocks,
	// and track how many pings were rejected (backpressure signal).
	go func() {
		for {
			ack, err := stream.Recv()
			if err != nil {
				return // stream closed / context cancelled
			}
			if ack.GetAccepted() {
				accepted.Add(1)
			} else {
				rejected.Add(1)
			}
		}
	}()

	// Throughput logger.
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		var last int64
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				s := sent.Load()
				log.Printf("simulator: sent=%d (%.0f/s) accepted=%d rejected=%d",
					s, float64(s-last)/5.0, accepted.Load(), rejected.Load())
				last = s
			}
		}
	}()

	// Sender loop: every interval, tick every vehicle and send its ping.
	// A single goroutine owns stream.Send (gRPC streams allow one concurrent
	// sender and one concurrent receiver, not more).
	ticker := time.NewTicker(*interval)
	defer ticker.Stop()
	log.Printf("simulator: streaming to %s every %s (Ctrl-C to stop)", *addr, *interval)

	for {
		select {
		case <-ctx.Done():
			log.Printf("simulator: shutting down; total sent=%d", sent.Load())
			_ = stream.CloseSend()
			time.Sleep(200 * time.Millisecond) // let final acks drain
			return
		case <-ticker.C:
			for _, v := range fleet {
				p := v.Tick(*interval)
				if err := stream.Send(toProto(p)); err != nil {
					log.Printf("simulator: send failed: %v", err)
					return
				}
				sent.Add(1)
			}
		}
	}
}

func toProto(p simulator.Ping) *fleetv1.GPSPing {
	return &fleetv1.GPSPing{
		VehicleId:       p.VehicleID,
		Latitude:        p.Lat,
		Longitude:       p.Lng,
		SpeedKmh:        p.SpeedKmh,
		Heading:         p.Heading,
		TimestampUnixMs: p.Time.UnixMilli(),
	}
}

// signalContext returns a context cancelled on SIGINT/SIGTERM.
func signalContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-ch
		cancel()
	}()
	return ctx, cancel
}

// pad zero-pads a vehicle index to 6 digits for stable, sortable IDs.
func pad(i int) string {
	const width = 6
	s := []byte("000000")
	n := i
	for pos := width - 1; pos >= 0 && n > 0; pos-- {
		s[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(s)
}
