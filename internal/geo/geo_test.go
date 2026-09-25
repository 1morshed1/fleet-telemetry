package geo

import (
	"math"
	"testing"
)

func TestHaversineKnownDistance(t *testing.T) {
	// London (51.5074, -0.1278) to Paris (48.8566, 2.3522) ~= 343 km.
	got := HaversineKm(51.5074, -0.1278, 48.8566, 2.3522)
	if math.Abs(got-343) > 5 {
		t.Fatalf("London->Paris = %.1f km, want ~343 km", got)
	}
}

func TestHaversineZero(t *testing.T) {
	if got := HaversineKm(10, 20, 10, 20); got != 0 {
		t.Fatalf("same point distance = %v, want 0", got)
	}
}

func TestMoveThenMeasure(t *testing.T) {
	// Moving 1 km north then measuring should give back ~1 km.
	lat, lng := 52.0, 13.0
	nLat, nLng := Move(lat, lng, 0 /*north*/, 1.0)
	d := HaversineKm(lat, lng, nLat, nLng)
	if math.Abs(d-1.0) > 0.01 {
		t.Fatalf("moved 1km north, measured %.4f km", d)
	}
	// Heading north increases latitude, leaves longitude ~unchanged.
	if nLat <= lat {
		t.Fatalf("north move should increase latitude: %v -> %v", lat, nLat)
	}
	if math.Abs(nLng-lng) > 1e-6 {
		t.Fatalf("north move changed longitude: %v -> %v", lng, nLng)
	}
}

func TestMoveEast(t *testing.T) {
	lat, lng := 52.0, 13.0
	_, nLng := Move(lat, lng, 90 /*east*/, 1.0)
	if nLng <= lng {
		t.Fatalf("east move should increase longitude: %v -> %v", lng, nLng)
	}
}

func TestSpeedKmh(t *testing.T) {
	// 1 km in 60 s = 60 km/h.
	if got := SpeedKmh(1.0, 60); math.Abs(got-60) > 1e-9 {
		t.Fatalf("SpeedKmh(1,60) = %v, want 60", got)
	}
	if got := SpeedKmh(1.0, 0); got != 0 {
		t.Fatalf("SpeedKmh with 0 seconds = %v, want 0", got)
	}
}
