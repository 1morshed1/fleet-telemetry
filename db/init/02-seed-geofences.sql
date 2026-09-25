-- Seed a few geofence zones around the fleet's spawn center (Berlin, 52.52, 13.405).
-- Polygons are ~1.5 km squares so the wandering simulated vehicles actually enter
-- and leave them, producing geofence + speed alerts to look at.
--
-- PostGIS note: WKT polygon coordinates are "longitude latitude" order, and the
-- ring must be closed (first point repeated as last). SRID 4326 = WGS84 lat/lng.

INSERT INTO geofence_zones (name, zone_type, boundary, speed_limit)
VALUES
  (
    'City Center Restricted', 'restricted',
    ST_GeomFromText('POLYGON((13.395 52.512, 13.415 52.512, 13.415 52.528, 13.395 52.528, 13.395 52.512))', 4326),
    50
  ),
  (
    'North Warehouse', 'warehouse',
    ST_GeomFromText('POLYGON((13.440 52.542, 13.460 52.542, 13.460 52.558, 13.440 52.558, 13.440 52.542))', 4326),
    30
  ),
  (
    'South Delivery Area', 'delivery_area',
    ST_GeomFromText('POLYGON((13.370 52.472, 13.390 52.472, 13.390 52.488, 13.370 52.488, 13.370 52.472))', 4326),
    60
  )
ON CONFLICT DO NOTHING;
