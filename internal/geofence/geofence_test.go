package geofence

import "testing"

// A ~1.5km square around Berlin center, matching db/init/02-seed-geofences.sql.
// Ring points are [lat, lng].
var berlinZone = Zone{
	ID: 1, Name: "City Center Restricted", Type: "restricted", SpeedLimit: 50,
	Ring: [][2]float64{
		{52.512, 13.395},
		{52.512, 13.415},
		{52.528, 13.415},
		{52.528, 13.395},
		{52.512, 13.395},
	},
}

func TestLookupInsideAndOutside(t *testing.T) {
	idx, err := Build([]Zone{berlinZone}, DefaultResolution)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if idx.NumCells() == 0 {
		t.Fatal("index has no cells; polyfill produced nothing")
	}

	// Center of the square -> inside.
	zones, err := idx.Lookup(52.520, 13.405)
	if err != nil {
		t.Fatalf("lookup inside: %v", err)
	}
	if len(zones) != 1 || zones[0].ID != 1 {
		t.Fatalf("point inside should match zone 1, got %v", zones)
	}

	// Far away -> outside.
	zones, err = idx.Lookup(48.85, 2.35) // Paris
	if err != nil {
		t.Fatalf("lookup outside: %v", err)
	}
	if len(zones) != 0 {
		t.Fatalf("far point should match no zone, got %v", zones)
	}
}

func TestSpeedLimitPreserved(t *testing.T) {
	idx, _ := Build([]Zone{berlinZone}, DefaultResolution)
	zones, _ := idx.Lookup(52.520, 13.405)
	if len(zones) != 1 || zones[0].SpeedLimit != 50 {
		t.Fatalf("expected speed limit 50, got %v", zones)
	}
}
