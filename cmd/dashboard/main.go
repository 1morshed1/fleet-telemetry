// Command dashboard serves the REST API (historical queries) and a WebSocket
// (live positions fed from Redis) plus the static Leaflet map page.
//
// Configuration (env vars):
//
//	HTTP_ADDR           listen address        (default :8080)
//	DATABASE_URL        postgres DSN          (default postgres://fleet:fleet@localhost:5432/fleet)
//	REDIS_ADDR          redis address         (default localhost:6379)
//	WEB_DIR             static assets dir     (default ./web)
//	BROADCAST_INTERVAL  live update cadence   (default 1s)
package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/1morshed1/fleet-telemetry/internal/config"
	"github.com/1morshed1/fleet-telemetry/internal/dashboard"
	"github.com/1morshed1/fleet-telemetry/internal/store"
)

func main() {
	httpAddr := config.String("HTTP_ADDR", ":8080")
	dsn := config.String("DATABASE_URL", "postgres://fleet:fleet@localhost:5432/fleet")
	redisAddr := config.String("REDIS_ADDR", "localhost:6379")
	webDir := config.String("WEB_DIR", "./web")
	interval := config.Duration("BROADCAST_INTERVAL", time.Second)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.New(ctx, dsn)
	if err != nil {
		log.Fatalf("dashboard: db: %v", err)
	}
	defer st.Close()

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	hub := dashboard.NewHub(rdb, interval)
	go hub.Run(ctx)

	srv := dashboard.NewServer(st, hub, webDir)
	httpSrv := &http.Server{Addr: httpAddr, Handler: srv.Routes()}

	go func() {
		log.Printf("dashboard: serving on %s (web dir %q, live every %s)", httpAddr, webDir, interval)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("dashboard: serve: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("dashboard: shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
}
