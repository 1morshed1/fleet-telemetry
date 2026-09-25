// Package live defines the shape of real-time position updates shared between
// the processor (publisher) and the dashboard (subscriber) over Redis Pub/Sub.
package live

// Channel is the Redis Pub/Sub channel carrying live vehicle positions.
const Channel = "live:positions"

// Position is one live update, JSON-encoded onto the channel. Kept small because
// it's published at the full ping rate.
type Position struct {
	VehicleID string  `json:"vehicle_id"`
	Lat       float64 `json:"lat"`
	Lng       float64 `json:"lng"`
	SpeedKmh  float64 `json:"speed_kmh"`
	Heading   float64 `json:"heading"`
	Status    string  `json:"status"` // "clear" | "violation"
	TimeMs    int64   `json:"time_ms"`
}
