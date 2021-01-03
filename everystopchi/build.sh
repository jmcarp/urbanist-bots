#!/bin/bash

set -euo pipefail

rm -f parcels-projected.db parcels.db

curl -L -o stops.zip https://data.cityofchicago.org/download/pxug-u72f/application%2Fzip
unzip stops.zip

ogr2ogr -f SQLite parcels.db CTA_BusStops.shp -t_srs EPSG:4326

sqlite3 parcels.db <<EOF
CREATE TABLE lots AS
SELECT
  ogc_fid AS id,
  public_nam AS address,
  dir AS direction,
  routesstpg AS routes,
  owlroutes AS owl_routes,
  CASE UPPER(pos)
    WHEN 'FS' THEN 'far side of intersection'
    WHEN 'NS' THEN 'near side of intersection'
    WHEN 'NT' THEN 'near side of T intersection'
    WHEN 'MB' THEN 'middle of block'
    WHEN 'MT' THEN 'middle of T intersection'
    WHEN 'FT' THEN 'far side of T intersection'
    WHEN 'TERM' THEN 'bus or rail terminal'
    ELSE NULL
  END AS position,
  point_y AS lat,
  point_x AS lon,
  0 AS tweeted
FROM cta_busstops
;
EOF
