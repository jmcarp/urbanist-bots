#!/bin/bash

set -euo pipefail

rm -f parcels-projected.db parcels.db

# https://openhartford-hartfordgis.opendata.arcgis.com/datasets/parcels
curl -o parcels.zip https://opendata.arcgis.com/datasets/67dfdd25d3b4450c9db1313c73c90f0c_11.zip
unzip parcels.zip

layer=$(ogrinfo Parcels.shp | grep '1: ' | awk '{print $2}')
source_table=$(ogrinfo Parcels.shp "${layer}" -so \
  | grep 'Layer name: ' \
  | sed 's/Layer name: //')
ogr2ogr -f SQLite parcels-projected.db Parcels.shp -t_srs EPSG:4326

# https://openhartford-hartfordgis.opendata.arcgis.com/datasets/address-points
curl -o addresses.csv https://opendata.arcgis.com/datasets/3495909b61ac4849965e529d4115ef86_9.csv

sqlite3 parcels-projected.db <<EOF
.import --csv addresses.csv addresses
EOF

ogr2ogr \
  -F SQLite \
  -dialect sqlite \
  parcels.db \
  parcels-projected.db \
  -nln lots \
  -sql "$(cat <<EOF
WITH parcels_by_pin AS (  -- Combine polygons with a shared pin
  SELECT
    gis_pin,
    ST_COLLECT(GEOMFROMWKB(Geometry)) AS geometry
  FROM ${source_table} parcels
  GROUP BY gis_pin
), addresses_by_pin AS (  -- Combine addresses with a shared pin
  SELECT
    camapin,
    GROUP_CONCAT(full_address, ', ') AS address
  FROM (
    SELECT DISTINCT
      camapin,
      full_address
    FROM addresses
    ORDER BY streetname, streetnum
  )
  GROUP BY camapin
)
SELECT
  gis_pin AS id,
  geometry,
  ROUND(X(ST_CENTROID(geometry)), 5) AS lon,
  ROUND(Y(ST_CENTROID(geometry)), 5) AS lat,
  address,
  0 AS tweeted
FROM parcels_by_pin
JOIN addresses_by_pin ON parcels_by_pin.gis_pin = addresses_by_pin.camapin
EOF
)"
