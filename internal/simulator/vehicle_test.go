package simulator

import (
	"testing"
	"time"

	"github.com/1morshed1/fleet-telemetry/internal/geo"
)

func TestNewVehicleWithinRadius(t *testing.T) {
	cLat, cLng, radius := 52.52, 13.405, 10.0
	for i := 0; i < 100; i++ {
		v := NewVehicle("v", cLat, cLng, radius, int64(i))
		d := geo.HaversineKm(cLat, cLng, v.Lat, v.Lng)
		if d > radius+0.001 {
			t.Fatalf("vehicle spawned %.2f km away, radius %.2f", d, radius)
		}
	}
}

func TestTickMovesVehicle(t *testing.T) {
	v := NewVehicle("v", 52.52, 13.405, 1, 42)
	start := struct{ lat, lng float64 }{v.Lat, v.Lng}
	p := v.Tick(time.Second)
	if p.VehicleID != "v" {
		t.Fatalf("ping vehicle id = %q", p.VehicleID)
	}
	if v.Lat == start.lat && v.Lng == start.lng {
		t.Fatal("Tick did not move the vehicle")
	}
	if p.SpeedKmh < 0 {
		t.Fatalf("negative reported speed: %v", p.SpeedKmh)
	}
}

func TestReproducibleBySeed(t *testing.T) {
	a := NewVehicle("v", 52.52, 13.405, 5, 7)
	b := NewVehicle("v", 52.52, 13.405, 5, 7)
	pa := a.Tick(time.Second)
	pb := b.Tick(time.Second)
	if pa.Lat != pb.Lat || pa.Lng != pb.Lng || pa.SpeedKmh != pb.SpeedKmh {
		t.Fatal("same seed should produce identical ticks")
	}
}

func TestHeadingStaysInRange(t *testing.T) {
	v := NewVehicle("v", 0, 0, 1, 3)
	for i := 0; i < 1000; i++ {
		v.Tick(time.Second)
		if v.Heading < 0 || v.Heading >= 360 {
			t.Fatalf("heading out of range: %v", v.Heading)
		}
	}
}
