package store

import (
	"context"
	"encoding/json"
	"time"
)

// VehiclePosition is a vehicle's most recent known position.
type VehiclePosition struct {
	VehicleID string    `json:"vehicle_id"`
	Lat       float64   `json:"lat"`
	Lng       float64   `json:"lng"`
	SpeedKmh  float64   `json:"speed_kmh"`
	Heading   float64   `json:"heading"`
	Status    string    `json:"status"`
	Time      time.Time `json:"time"`
}

// LatestPositions returns the last known position for each vehicle seen in the
// recent window. DISTINCT ON (vehicle_id) with ORDER BY vehicle_id, time DESC is
// the Postgres idiom for "latest row per group". ST_X = longitude, ST_Y = latitude.
func (s *Store) LatestPositions(ctx context.Context, window time.Duration) ([]VehiclePosition, error) {
	const q = `
SELECT DISTINCT ON (vehicle_id)
       vehicle_id, ST_Y(location), ST_X(location), speed_kmh, heading, geofence_status, time
FROM gps_events
WHERE time > now() - $1::interval
ORDER BY vehicle_id, time DESC`
	rows, err := s.pool.Query(ctx, q, window.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []VehiclePosition
	for rows.Next() {
		var p VehiclePosition
		if err := rows.Scan(&p.VehicleID, &p.Lat, &p.Lng, &p.SpeedKmh, &p.Heading, &p.Status, &p.Time); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// HistoryPoint is one sample in a vehicle's history.
type HistoryPoint struct {
	Time     time.Time `json:"time"`
	Lat      float64   `json:"lat"`
	Lng      float64   `json:"lng"`
	SpeedKmh float64   `json:"speed_kmh"`
}

// VehicleHistory returns the route as a GeoJSON LineString plus per-point speed
// data over a time range. The route is built on demand with ST_MakeLine (see
// docs/05-timescaledb.md for why it isn't in the continuous aggregate).
func (s *Store) VehicleHistory(ctx context.Context, vehicleID string, from, to time.Time) (json.RawMessage, []HistoryPoint, error) {
	const routeQ = `
SELECT COALESCE(ST_AsGeoJSON(ST_MakeLine(location ORDER BY time)), 'null')
FROM gps_events
WHERE vehicle_id = $1 AND time BETWEEN $2 AND $3`
	var route json.RawMessage
	if err := s.pool.QueryRow(ctx, routeQ, vehicleID, from, to).Scan(&route); err != nil {
		return nil, nil, err
	}

	const ptQ = `
SELECT time, ST_Y(location), ST_X(location), speed_kmh
FROM gps_events
WHERE vehicle_id = $1 AND time BETWEEN $2 AND $3
ORDER BY time`
	rows, err := s.pool.Query(ctx, ptQ, vehicleID, from, to)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	var pts []HistoryPoint
	for rows.Next() {
		var p HistoryPoint
		if err := rows.Scan(&p.Time, &p.Lat, &p.Lng, &p.SpeedKmh); err != nil {
			return nil, nil, err
		}
		pts = append(pts, p)
	}
	return route, pts, rows.Err()
}

// GeofencesGeoJSON returns all zones as a GeoJSON FeatureCollection ready for
// Leaflet. Each feature carries the zone's properties.
func (s *Store) GeofencesGeoJSON(ctx context.Context) (json.RawMessage, error) {
	const q = `
SELECT COALESCE(
  json_build_object(
    'type', 'FeatureCollection',
    'features', COALESCE(json_agg(
      json_build_object(
        'type', 'Feature',
        'geometry', ST_AsGeoJSON(boundary)::json,
        'properties', json_build_object(
          'id', id, 'name', name, 'zone_type', zone_type, 'speed_limit', speed_limit)
      )), '[]'::json)
  ), json_build_object('type','FeatureCollection','features','[]'::json))
FROM geofence_zones`
	var fc json.RawMessage
	if err := s.pool.QueryRow(ctx, q).Scan(&fc); err != nil {
		return nil, err
	}
	return fc, nil
}

// Alert is a unified alert row for the dashboard feed.
type Alert struct {
	Type      string    `json:"type"` // "speed" | "geofence"
	Time      time.Time `json:"time"`
	VehicleID string    `json:"vehicle_id"`
	Lat       float64   `json:"lat"`
	Lng       float64   `json:"lng"`
	Detail    string    `json:"detail"`
}

// RecentSpeedAlerts returns the newest speed alerts in a time range.
func (s *Store) RecentSpeedAlerts(ctx context.Context, from, to time.Time, limit int) ([]Alert, error) {
	const q = `
SELECT time, vehicle_id, speed_kmh, limit_kmh, ST_Y(location), ST_X(location)
FROM speed_alerts WHERE time BETWEEN $1 AND $2 ORDER BY time DESC LIMIT $3`
	rows, err := s.pool.Query(ctx, q, from, to, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Alert
	for rows.Next() {
		var a Alert
		var speed, limit float64
		if err := rows.Scan(&a.Time, &a.VehicleID, &speed, &limit, &a.Lat, &a.Lng); err != nil {
			return nil, err
		}
		a.Type = "speed"
		a.Detail = formatSpeed(speed, limit)
		out = append(out, a)
	}
	return out, rows.Err()
}

// RecentGeofenceAlerts returns the newest geofence alerts in a time range.
func (s *Store) RecentGeofenceAlerts(ctx context.Context, from, to time.Time, limit int) ([]Alert, error) {
	const q = `
SELECT time, vehicle_id, zone_name, ST_Y(location), ST_X(location)
FROM geofence_alerts WHERE time BETWEEN $1 AND $2 ORDER BY time DESC LIMIT $3`
	rows, err := s.pool.Query(ctx, q, from, to, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Alert
	for rows.Next() {
		var a Alert
		var zone string
		if err := rows.Scan(&a.Time, &a.VehicleID, &zone, &a.Lat, &a.Lng); err != nil {
			return nil, err
		}
		a.Type = "geofence"
		a.Detail = "entered " + zone
		out = append(out, a)
	}
	return out, rows.Err()
}

func formatSpeed(speed, limit float64) string {
	return "speeding " + trim(speed) + " km/h (limit " + trim(limit) + ")"
}

// trim formats a float without trailing noise (e.g. 123.4).
func trim(f float64) string {
	b, _ := json.Marshal(round1(f))
	return string(b)
}

func round1(f float64) float64 {
	return float64(int(f*10+0.5)) / 10
}
