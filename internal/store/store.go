// Package store is the TimescaleDB/PostGIS data-access layer. It uses pgx
// (pgxpool) for connection pooling. Geometry is stored as PostGIS points;
// callers pass plain lat/lng and the SQL wraps them in ST_MakePoint.
package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/1morshed1/fleet-telemetry/internal/geofence"
)

// Store wraps a pgx connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// New opens a pooled connection to the database and pings it.
func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close releases the pool.
func (s *Store) Close() { s.pool.Close() }

// Pool exposes the underlying pool for the dashboard's read queries.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Event is one processed ping ready to persist.
type Event struct {
	Time           time.Time
	VehicleID      string
	Lat, Lng       float64
	SpeedKmh       float64
	Heading        float64
	GeofenceStatus string
}

// InsertEvents writes a batch of gps_events in a single round-trip using
// unnest(). This is far cheaper than one INSERT per row. We can't use pgx
// CopyFrom directly because each row needs ST_MakePoint to build geometry.
func (s *Store) InsertEvents(ctx context.Context, evs []Event) error {
	if len(evs) == 0 {
		return nil
	}
	n := len(evs)
	times := make([]time.Time, n)
	vids := make([]string, n)
	lngs := make([]float64, n)
	lats := make([]float64, n)
	speeds := make([]float64, n)
	headings := make([]float64, n)
	statuses := make([]string, n)
	for i, e := range evs {
		times[i] = e.Time
		vids[i] = e.VehicleID
		lngs[i] = e.Lng
		lats[i] = e.Lat
		speeds[i] = e.SpeedKmh
		headings[i] = e.Heading
		statuses[i] = e.GeofenceStatus
	}

	const q = `
INSERT INTO gps_events (time, vehicle_id, location, speed_kmh, heading, geofence_status)
SELECT t, v, ST_SetSRID(ST_MakePoint(lng, lat), 4326), s, h, st
FROM unnest($1::timestamptz[], $2::text[], $3::float8[], $4::float8[],
            $5::float8[], $6::float8[], $7::text[])
     AS u(t, v, lng, lat, s, h, st)`
	_, err := s.pool.Exec(ctx, q, times, vids, lngs, lats, speeds, headings, statuses)
	if err != nil {
		return fmt.Errorf("insert events: %w", err)
	}
	return nil
}

// InsertSpeedAlert records a speeding event.
func (s *Store) InsertSpeedAlert(ctx context.Context, t time.Time, vehicleID string, speed, limit, lat, lng float64) error {
	const q = `
INSERT INTO speed_alerts (time, vehicle_id, speed_kmh, limit_kmh, location)
VALUES ($1, $2, $3, $4, ST_SetSRID(ST_MakePoint($5, $6), 4326))`
	_, err := s.pool.Exec(ctx, q, t, vehicleID, speed, limit, lng, lat)
	return err
}

// InsertGeofenceAlert records a geofence violation (entry into a watched zone).
func (s *Store) InsertGeofenceAlert(ctx context.Context, t time.Time, vehicleID string, zoneID int, zoneName string, lat, lng float64) error {
	const q = `
INSERT INTO geofence_alerts (time, vehicle_id, zone_id, zone_name, location)
VALUES ($1, $2, $3, $4, ST_SetSRID(ST_MakePoint($5, $6), 4326))`
	_, err := s.pool.Exec(ctx, q, t, vehicleID, zoneID, zoneName, lng, lat)
	return err
}

// LoadGeofences reads all zones and returns them as geofence.Zone (ring as
// [lat,lng] pairs) ready to build an H3 index. Boundary is fetched as GeoJSON
// and parsed, avoiding fragile WKT string parsing.
func (s *Store) LoadGeofences(ctx context.Context) ([]geofence.Zone, error) {
	const q = `
SELECT id, name, zone_type, COALESCE(speed_limit, 0),
       ST_AsGeoJSON(boundary)
FROM geofence_zones`
	rows, err := s.pool.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var zones []geofence.Zone
	for rows.Next() {
		var (
			z       geofence.Zone
			geoJSON string
		)
		if err := rows.Scan(&z.ID, &z.Name, &z.Type, &z.SpeedLimit, &geoJSON); err != nil {
			return nil, err
		}
		ring, err := parsePolygonRing(geoJSON)
		if err != nil {
			return nil, fmt.Errorf("zone %d (%s): %w", z.ID, z.Name, err)
		}
		z.Ring = ring
		zones = append(zones, z)
	}
	return zones, rows.Err()
}

// parsePolygonRing extracts the exterior ring from a GeoJSON Polygon, converting
// GeoJSON [lng,lat] order into our [lat,lng] pairs.
func parsePolygonRing(geoJSON string) ([][2]float64, error) {
	var g struct {
		Type        string         `json:"type"`
		Coordinates [][][2]float64 `json:"coordinates"`
	}
	if err := json.Unmarshal([]byte(geoJSON), &g); err != nil {
		return nil, err
	}
	if len(g.Coordinates) == 0 {
		return nil, fmt.Errorf("polygon has no rings")
	}
	ext := g.Coordinates[0] // exterior ring
	ring := make([][2]float64, 0, len(ext))
	for _, p := range ext {
		// p is [lng, lat] in GeoJSON.
		ring = append(ring, [2]float64{p[1], p[0]})
	}
	return ring, nil
}
