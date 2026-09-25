package processor

import (
	"context"
	"testing"
	"time"

	"github.com/1morshed1/fleet-telemetry/internal/geofence"
	"github.com/1morshed1/fleet-telemetry/internal/store"
	fleetv1 "github.com/1morshed1/fleet-telemetry/proto/fleet/v1"
)

// fakeStore records alerts and events without a database.
type fakeStore struct {
	events         []store.Event
	speedAlerts    int
	geofenceAlerts int
}

func (f *fakeStore) InsertEvents(_ context.Context, evs []store.Event) error {
	f.events = append(f.events, evs...)
	return nil
}
func (f *fakeStore) InsertSpeedAlert(_ context.Context, _ time.Time, _ string, _, _, _, _ float64) error {
	f.speedAlerts++
	return nil
}
func (f *fakeStore) InsertGeofenceAlert(_ context.Context, _ time.Time, _ string, _ int, _ string, _, _ float64) error {
	f.geofenceAlerts++
	return nil
}

func restrictedIndex(t *testing.T) *geofence.Index {
	t.Helper()
	zone := geofence.Zone{
		ID: 1, Name: "Restricted", Type: "restricted", SpeedLimit: 50,
		Ring: [][2]float64{
			{52.512, 13.395}, {52.512, 13.415}, {52.528, 13.415}, {52.528, 13.395}, {52.512, 13.395},
		},
	}
	idx, err := geofence.Build([]geofence.Zone{zone}, geofence.DefaultResolution)
	if err != nil {
		t.Fatalf("build index: %v", err)
	}
	return idx
}

func ping(vid string, lat, lng, speed float64, tMs int64) *fleetv1.GPSPing {
	return &fleetv1.GPSPing{
		VehicleId: vid, Latitude: lat, Longitude: lng, SpeedKmh: speed, TimestampUnixMs: tMs,
	}
}

func TestGeofenceAlertOnEntryOnly(t *testing.T) {
	fs := &fakeStore{}
	p := New(nil, fs, restrictedIndex(t), nil, DefaultConfig())
	ctx := context.Background()

	// Two consecutive pings INSIDE the restricted zone: only the first (entry)
	// should raise a geofence alert.
	base := time.Now().UnixMilli()
	e1 := p.handle(ctx, ping("v", 52.520, 13.405, 10, base))
	_ = p.handle(ctx, ping("v", 52.521, 13.406, 10, base+1000))

	if e1.GeofenceStatus != "violation" {
		t.Fatalf("expected violation status, got %q", e1.GeofenceStatus)
	}
	if fs.geofenceAlerts != 1 {
		t.Fatalf("expected 1 geofence alert (entry only), got %d", fs.geofenceAlerts)
	}
}

func TestSpeedAlertOnTransition(t *testing.T) {
	fs := &fakeStore{}
	// No geofence match here (far from the zone) so default limit 120 applies.
	idx, _ := geofence.Build(nil, geofence.DefaultResolution)
	p := New(nil, fs, idx, nil, DefaultConfig())
	ctx := context.Background()

	// First ping: no previous -> uses reported speed (slow), no alert.
	base := time.Now().UnixMilli()
	p.handle(ctx, ping("v", 10.0, 10.0, 30, base))

	// Second ping: jump ~1 km in 1 s => 3600 km/h calculated -> speeding.
	nLat := 10.0 + 1.0/111.0
	p.handle(ctx, ping("v", nLat, 10.0, 30, base+1000))
	if fs.speedAlerts != 1 {
		t.Fatalf("expected 1 speed alert on transition, got %d", fs.speedAlerts)
	}

	// Third ping: barely moves -> not speeding -> resets, still only 1 alert.
	p.handle(ctx, ping("v", nLat+0.000001, 10.0, 30, base+2000))
	if fs.speedAlerts != 1 {
		t.Fatalf("expected still 1 speed alert after slowing, got %d", fs.speedAlerts)
	}
}
