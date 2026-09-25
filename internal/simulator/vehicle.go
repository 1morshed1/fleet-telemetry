// Package simulator models a fleet of vehicles moving around a map. Each
// vehicle is a small state machine: every Tick it advances along its heading,
// occasionally turns, varies its speed within its road type's range, and adds
// GPS jitter so the data looks realistically noisy.
package simulator

import (
	"math/rand"
	"time"

	"github.com/1morshed1/fleet-telemetry/internal/geo"
)

// RouteType controls how fast a vehicle tends to move and how often it turns.
type RouteType string

const (
	Highway RouteType = "highway"
	Urban   RouteType = "urban"
	Rural   RouteType = "rural"
)

// speedRange is the [min,max] cruising speed (km/h) per route type.
var speedRange = map[RouteType][2]float64{
	Highway: {90, 130},
	Urban:   {20, 60},
	Rural:   {50, 90},
}

// turnChance is the per-tick probability a vehicle changes heading. Urban
// vehicles turn often (intersections); highway vehicles rarely do.
var turnChance = map[RouteType]float64{
	Highway: 0.02,
	Urban:   0.25,
	Rural:   0.08,
}

// Ping is the simulator's own output type. We keep it separate from the
// generated protobuf GPSPing so the domain model doesn't depend on wire code;
// the cmd layer maps Ping -> protobuf when sending.
type Ping struct {
	VehicleID string
	Lat       float64
	Lng       float64
	SpeedKmh  float64
	Heading   float64
	Time      time.Time
}

// Vehicle is one simulated device. It is owned by a single goroutine, so its
// own *rand.Rand is used instead of the global (thread-unsafe) rand functions.
type Vehicle struct {
	ID        string
	Lat       float64
	Lng       float64
	Speed     float64 // current cruising speed km/h
	Heading   float64 // degrees, 0 = north
	RouteType RouteType

	rng *rand.Rand
}

// NewVehicle creates a vehicle at a random point within radiusKm of a center,
// with a random route type, heading, and speed. seed makes a fleet reproducible.
func NewVehicle(id string, centerLat, centerLng, radiusKm float64, seed int64) *Vehicle {
	rng := rand.New(rand.NewSource(seed))

	routes := []RouteType{Highway, Urban, Rural}
	rt := routes[rng.Intn(len(routes))]

	// Random offset within the spawn radius.
	bearing := rng.Float64() * 360
	dist := rng.Float64() * radiusKm
	lat, lng := geo.Move(centerLat, centerLng, bearing, dist)

	sr := speedRange[rt]
	speed := sr[0] + rng.Float64()*(sr[1]-sr[0])

	return &Vehicle{
		ID:        id,
		Lat:       lat,
		Lng:       lng,
		Speed:     speed,
		Heading:   rng.Float64() * 360,
		RouteType: rt,
		rng:       rng,
	}
}

// Tick advances the vehicle by one interval and returns the GPS ping a real
// device would emit. It mutates the vehicle's position/heading/speed.
func (v *Vehicle) Tick(interval time.Duration) Ping {
	// Maybe turn: nudge heading by up to +/-45 degrees.
	if v.rng.Float64() < turnChance[v.RouteType] {
		v.Heading += (v.rng.Float64() - 0.5) * 90
		v.Heading = wrapHeading(v.Heading)
	}

	// Maybe vary speed a little, staying within the route's range (plus an
	// occasional overshoot so speeding alerts actually fire in demos).
	v.Speed += (v.rng.Float64() - 0.5) * 10
	v.Speed = clampSpeed(v.Speed, v.RouteType, v.rng)

	// Advance position by distance = speed * time.
	distanceKm := v.Speed * interval.Hours()
	v.Lat, v.Lng = geo.Move(v.Lat, v.Lng, v.Heading, distanceKm)

	// GPS jitter: real receivers wobble by a few meters. ~0.00005 deg ~= 5 m.
	v.Lat += (v.rng.Float64() - 0.5) * 0.0001
	v.Lng += (v.rng.Float64() - 0.5) * 0.0001

	// The device reports its speed with sensor noise too (+/- ~5 km/h).
	reportedSpeed := v.Speed + (v.rng.Float64()-0.5)*10
	if reportedSpeed < 0 {
		reportedSpeed = 0
	}

	return Ping{
		VehicleID: v.ID,
		Lat:       v.Lat,
		Lng:       v.Lng,
		SpeedKmh:  reportedSpeed,
		Heading:   v.Heading,
		Time:      time.Now(),
	}
}

func wrapHeading(h float64) float64 {
	for h < 0 {
		h += 360
	}
	for h >= 360 {
		h -= 360
	}
	return h
}

// clampSpeed keeps speed within the route's range, but ~2% of the time lets it
// run 10-30 km/h over the top of the range to simulate speeding.
func clampSpeed(speed float64, rt RouteType, rng *rand.Rand) float64 {
	sr := speedRange[rt]
	if rng.Float64() < 0.02 {
		return sr[1] + 10 + rng.Float64()*20 // deliberate speeding
	}
	if speed < sr[0] {
		return sr[0]
	}
	if speed > sr[1] {
		return sr[1]
	}
	return speed
}
