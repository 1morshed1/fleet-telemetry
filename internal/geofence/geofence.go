// Package geofence turns a set of geofence polygons into an O(1) lookup using
// Uber's H3 hexagonal grid. Instead of testing each ping against every polygon
// edge (slow), we precompute the set of H3 cells that tile each polygon, then a
// ping's cell is a single hash lookup. See docs/04-processing-geospatial.md.
package geofence

import (
	"fmt"

	h3 "github.com/uber/h3-go/v4"
)

// DefaultResolution 9 ~= 174 m average hexagon edge (~0.1 km^2 cells). Fine
// enough for street-level geofences without exploding the cell count.
const DefaultResolution = 9

// Zone is a geofence: a named polygon with an optional speed limit.
type Zone struct {
	ID         int
	Name       string
	Type       string       // restricted | warehouse | delivery_area
	SpeedLimit float64      // km/h; 0 means "no explicit limit"
	Ring       [][2]float64 // polygon exterior ring as [lat, lng] pairs
}

// Index maps H3 cells to the zones that cover them.
type Index struct {
	res         int
	cellToZones map[h3.Cell][]int
	zones       map[int]Zone
}

// Build precomputes the H3 tiling for every zone at the given resolution.
func Build(zones []Zone, resolution int) (*Index, error) {
	idx := &Index{
		res:         resolution,
		cellToZones: make(map[h3.Cell][]int),
		zones:       make(map[int]Zone, len(zones)),
	}
	for _, z := range zones {
		idx.zones[z.ID] = z

		loop := make(h3.GeoLoop, 0, len(z.Ring))
		for _, p := range z.Ring {
			loop = append(loop, h3.NewLatLng(p[0], p[1]))
		}
		poly := h3.GeoPolygon{GeoLoop: loop}

		cells, err := h3.PolygonToCells(poly, resolution)
		if err != nil {
			return nil, fmt.Errorf("polyfill zone %d (%s): %w", z.ID, z.Name, err)
		}
		for _, c := range cells {
			idx.cellToZones[c] = append(idx.cellToZones[c], z.ID)
		}
	}
	return idx, nil
}

// Lookup returns the zones whose H3 tiling contains the point. This is the hot
// path: one LatLngToCell + one map lookup, regardless of how many zones or how
// complex their polygons are.
func (i *Index) Lookup(lat, lng float64) ([]Zone, error) {
	cell, err := h3.LatLngToCell(h3.NewLatLng(lat, lng), i.res)
	if err != nil {
		return nil, err
	}
	ids := i.cellToZones[cell]
	if len(ids) == 0 {
		return nil, nil
	}
	out := make([]Zone, 0, len(ids))
	for _, id := range ids {
		out = append(out, i.zones[id])
	}
	return out, nil
}

// NumCells reports how many distinct H3 cells the index holds (useful for logs
// and understanding memory footprint).
func (i *Index) NumCells() int { return len(i.cellToZones) }
