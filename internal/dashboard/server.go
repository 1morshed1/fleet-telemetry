package dashboard

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"

	"github.com/1morshed1/fleet-telemetry/internal/store"
)

// Server holds dependencies for the HTTP handlers.
type Server struct {
	store  *store.Store
	hub    *Hub
	webDir string
	up     websocket.Upgrader
}

// NewServer builds the dashboard HTTP server. webDir is the directory of static
// assets (the Leaflet page); "" disables static serving.
func NewServer(st *store.Store, hub *Hub, webDir string) *Server {
	return &Server{
		store:  st,
		hub:    hub,
		webDir: webDir,
		up: websocket.Upgrader{
			// Local/dev: accept any origin. In production, check r.Header["Origin"].
			CheckOrigin: func(_ *http.Request) bool { return true },
		},
	}
}

// Routes returns the configured HTTP handler.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/vehicles", s.handleVehicles)
	mux.HandleFunc("/api/v1/vehicles/", s.handleVehicleHistory) // /vehicles/{id}/history
	mux.HandleFunc("/api/v1/geofences", s.handleGeofences)
	mux.HandleFunc("/api/v1/alerts", s.handleAlerts)
	mux.HandleFunc("/api/v1/live", s.handleLive)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })

	if s.webDir != "" {
		mux.Handle("/", http.FileServer(http.Dir(s.webDir)))
	}
	return mux
}

// GET /api/v1/vehicles — latest known position of every vehicle (last 5 min).
func (s *Server) handleVehicles(w http.ResponseWriter, r *http.Request) {
	positions, err := s.store.LatestPositions(r.Context(), 5*time.Minute)
	if err != nil {
		httpError(w, err)
		return
	}
	writeJSON(w, positions)
}

// GET /api/v1/vehicles/{id}/history?from=RFC3339&to=RFC3339
func (s *Server) handleVehicleHistory(w http.ResponseWriter, r *http.Request) {
	// Path is /api/v1/vehicles/{id}/history
	rest := r.URL.Path[len("/api/v1/vehicles/"):]
	id, tail := splitFirst(rest, '/')
	if id == "" || tail != "history" {
		http.NotFound(w, r)
		return
	}
	from := parseTime(r.URL.Query().Get("from"), time.Now().Add(-time.Hour))
	to := parseTime(r.URL.Query().Get("to"), time.Now())

	route, points, err := s.store.VehicleHistory(r.Context(), id, from, to)
	if err != nil {
		httpError(w, err)
		return
	}
	writeJSON(w, map[string]any{
		"vehicle_id": id,
		"route":      route, // GeoJSON LineString (or null)
		"points":     points,
	})
}

// GET /api/v1/geofences — GeoJSON FeatureCollection.
func (s *Server) handleGeofences(w http.ResponseWriter, r *http.Request) {
	fc, err := s.store.GeofencesGeoJSON(r.Context())
	if err != nil {
		httpError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(fc)
}

// GET /api/v1/alerts?type=speed|geofence|all&from=&to=
func (s *Server) handleAlerts(w http.ResponseWriter, r *http.Request) {
	typ := r.URL.Query().Get("type")
	if typ == "" {
		typ = "all"
	}
	from := parseTime(r.URL.Query().Get("from"), time.Now().Add(-24*time.Hour))
	to := parseTime(r.URL.Query().Get("to"), time.Now())
	const limit = 200

	var alerts []store.Alert
	if typ == "speed" || typ == "all" {
		a, err := s.store.RecentSpeedAlerts(r.Context(), from, to, limit)
		if err != nil {
			httpError(w, err)
			return
		}
		alerts = append(alerts, a...)
	}
	if typ == "geofence" || typ == "all" {
		a, err := s.store.RecentGeofenceAlerts(r.Context(), from, to, limit)
		if err != nil {
			httpError(w, err)
			return
		}
		alerts = append(alerts, a...)
	}
	writeJSON(w, alerts)
}

// GET /api/v1/live — WebSocket of live positions.
func (s *Server) handleLive(w http.ResponseWriter, r *http.Request) {
	conn, err := s.up.Upgrade(w, r, nil)
	if err != nil {
		return // Upgrade already wrote an error response
	}
	s.hub.serveWS(conn)
}

// --- helpers ---

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("dashboard: encode: %v", err)
	}
}

func httpError(w http.ResponseWriter, err error) {
	log.Printf("dashboard: query error: %v", err)
	http.Error(w, "internal error", http.StatusInternalServerError)
}

// parseTime parses an RFC3339 timestamp, falling back to def on empty/invalid.
func parseTime(s string, def time.Time) time.Time {
	if s == "" {
		return def
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t
	}
	return def
}

// splitFirst splits s at the first occurrence of sep into (before, after).
func splitFirst(s string, sep byte) (string, string) {
	for i := 0; i < len(s); i++ {
		if s[i] == sep {
			return s[:i], s[i+1:]
		}
	}
	return s, ""
}
