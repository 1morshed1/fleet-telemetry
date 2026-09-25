// Package geo holds small, dependency-free geospatial helpers shared across
// services: great-circle distance and dead-reckoning movement. Keeping this
// logic in one place means the simulator (which moves vehicles) and the
// processor (which recomputes speed from positions) agree on the math.
package geo

import "math"

const (
	earthRadiusKm = 6371.0
	// kmPerDegreeLat is the distance of one degree of latitude, roughly
	// constant everywhere (~111 km). Longitude degrees shrink toward the poles,
	// so we scale them by cos(latitude).
	kmPerDegreeLat = 111.0
)

func deg2rad(d float64) float64 { return d * math.Pi / 180 }

// HaversineKm returns the great-circle distance in kilometers between two
// lat/lng points. This is the correct way to measure distance on a sphere;
// naive Euclidean distance on lat/lng is wrong because longitude degrees are
// not a fixed length.
func HaversineKm(lat1, lng1, lat2, lng2 float64) float64 {
	dLat := deg2rad(lat2 - lat1)
	dLng := deg2rad(lng2 - lng1)
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(deg2rad(lat1))*math.Cos(deg2rad(lat2))*
			math.Sin(dLng/2)*math.Sin(dLng/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return earthRadiusKm * c
}

// Move returns a new lat/lng after travelling distanceKm along a compass
// heading (degrees, 0 = north, 90 = east, clockwise). This is "dead
// reckoning": project a point from a start position, bearing, and distance.
//
// It uses a flat-earth approximation, which is accurate enough for the small
// per-tick steps a vehicle makes (tens of meters). For long distances you'd
// use the spherical forward geodesic formula instead.
func Move(lat, lng, headingDeg, distanceKm float64) (float64, float64) {
	h := deg2rad(headingDeg)
	newLat := lat + (distanceKm/kmPerDegreeLat)*math.Cos(h)
	// Guard against cos(lat)=0 at the poles (division by zero).
	cosLat := math.Cos(deg2rad(lat))
	if math.Abs(cosLat) < 1e-9 {
		cosLat = 1e-9
	}
	newLng := lng + (distanceKm/(kmPerDegreeLat*cosLat))*math.Sin(h)
	return newLat, newLng
}

// SpeedKmh computes speed from a distance and an elapsed duration in seconds.
// Returns 0 for non-positive durations to avoid divide-by-zero and nonsensical
// infinite speeds when two pings share a timestamp.
func SpeedKmh(distanceKm float64, seconds float64) float64 {
	if seconds <= 0 {
		return 0
	}
	return distanceKm / (seconds / 3600.0)
}
