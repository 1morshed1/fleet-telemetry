// Command ingestion is the gRPC server that receives GPS ping streams, batches
// them, and produces to Kafka. It also exposes Prometheus metrics.
//
// Configuration (env vars):
//
//	GRPC_ADDR       gRPC listen address       (default :50051)
//	METRICS_ADDR    Prometheus listen address (default :2112)
//	KAFKA_BROKERS   comma-separated brokers   (default localhost:9092)
package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

	"github.com/1morshed1/fleet-telemetry/internal/config"
	"github.com/1morshed1/fleet-telemetry/internal/ingestion"
	fkafka "github.com/1morshed1/fleet-telemetry/internal/kafka"
	fleetv1 "github.com/1morshed1/fleet-telemetry/proto/fleet/v1"
)

func main() {
	grpcAddr := config.String("GRPC_ADDR", ":50051")
	metricsAddr := config.String("METRICS_ADDR", ":2112")
	brokers := strings.Split(config.String("KAFKA_BROKERS", "localhost:9092"), ",")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Kafka writer for the raw pings topic.
	kw := fkafka.NewWriter(brokers, fkafka.TopicGPSPings)
	defer kw.Close()

	srv := ingestion.NewServer(ctx, kw, ingestion.DefaultConfig())

	// gRPC server.
	grpcServer := grpc.NewServer()
	fleetv1.RegisterFleetIngestionServer(grpcServer, srv)

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("ingestion: listen %s: %v", grpcAddr, err)
	}

	// Metrics HTTP server.
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	metricsSrv := &http.Server{Addr: metricsAddr, Handler: mux}
	go func() {
		log.Printf("ingestion: metrics on %s/metrics", metricsAddr)
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("ingestion: metrics server: %v", err)
		}
	}()

	go func() {
		log.Printf("ingestion: gRPC listening on %s, producing to Kafka %v", grpcAddr, brokers)
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("ingestion: grpc serve: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("ingestion: shutting down")
	grpcServer.GracefulStop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = metricsSrv.Shutdown(shutdownCtx)
}
