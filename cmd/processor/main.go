// Command processor consumes GPS pings from Kafka, runs geofencing + speed
// detection, writes enriched events and alerts to TimescaleDB, and publishes
// live positions to Redis.
//
// Configuration (env vars):
//
//	KAFKA_BROKERS   comma-separated brokers  (default localhost:9092)
//	KAFKA_GROUP     consumer group id        (default fleet-processor)
//	DATABASE_URL    postgres DSN             (default postgres://fleet:fleet@localhost:5432/fleet)
//	REDIS_ADDR      redis address            (default localhost:6379)
//	METRICS_ADDR    prometheus listen addr   (default :2113)
package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"strings"
	"syscall"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"github.com/1morshed1/fleet-telemetry/internal/config"
	"github.com/1morshed1/fleet-telemetry/internal/geofence"
	fkafka "github.com/1morshed1/fleet-telemetry/internal/kafka"
	"github.com/1morshed1/fleet-telemetry/internal/processor"
	"github.com/1morshed1/fleet-telemetry/internal/store"
)

func main() {
	brokers := strings.Split(config.String("KAFKA_BROKERS", "localhost:9092"), ",")
	group := config.String("KAFKA_GROUP", "fleet-processor")
	dsn := config.String("DATABASE_URL", "postgres://fleet:fleet@localhost:5432/fleet")
	redisAddr := config.String("REDIS_ADDR", "localhost:6379")
	metricsAddr := config.String("METRICS_ADDR", ":2113")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Database.
	st, err := store.New(ctx, dsn)
	if err != nil {
		log.Fatalf("processor: db: %v", err)
	}
	defer st.Close()

	// Load geofences and build the H3 index once at startup.
	zones, err := st.LoadGeofences(ctx)
	if err != nil {
		log.Fatalf("processor: load geofences: %v", err)
	}
	idx, err := geofence.Build(zones, geofence.DefaultResolution)
	if err != nil {
		log.Fatalf("processor: build geofence index: %v", err)
	}
	log.Printf("processor: loaded %d geofences -> %d H3 cells (res %d)",
		len(zones), idx.NumCells(), geofence.DefaultResolution)

	// Redis for live position fan-out.
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	// Kafka consumer.
	reader := fkafka.NewReader(brokers, fkafka.TopicGPSPings, group)
	defer reader.Close()

	proc := processor.New(reader, st, idx, rdb, processor.DefaultConfig())

	// Metrics.
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	metricsSrv := &http.Server{Addr: metricsAddr, Handler: mux}
	go func() {
		log.Printf("processor: metrics on %s/metrics", metricsAddr)
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("processor: metrics server: %v", err)
		}
	}()

	log.Printf("processor: consuming %s from %v (group %s)", fkafka.TopicGPSPings, brokers, group)
	if err := proc.Run(ctx); err != nil && err != context.Canceled {
		log.Printf("processor: run: %v", err)
	}
	log.Println("processor: stopped")
}
