#!/bin/bash

set -euo pipefail

rm -f parcels-projected.db parcels.db

curl -O https://widget.charlottesville.org/gis/zip_download/parcel_area.zip
unzip parcel_area.zip

layer=$(ogrinfo parcel_area_*.shp | grep '1: ' | awk '{print $2}')
source_table=$(ogrinfo parcel_area_*.shp "${layer}" -so \
  | grep 'Layer name: ' \
  | sed 's/Layer name: //')
ogr2ogr -f SQLite parcels-projected.db parcel_area_*.shp -t_srs EPSG:4326 -select PIN,GPIN

# https://opendata.charlottesville.org/datasets/real-estate-base-data
curl -o real_estate.csv https://opendata.arcgis.com/datasets/bc72d0590bf940ff952ab113f10a36a8_8.csv
sqlite3 parcels-projected.db <<EOF
CREATE TABLE real_estate (
  "RecordID_Int" INTEGER,
  "ParcelNumber" INTEGER,
  "StreetNumber" TEXT,
  "StreetName" TEXT,
  "Unit" TEXT,
  "StateCode" TEXT,
  "TaxType" TEXT,
  "Zone" TEXT,
  "TaxDist" TEXT,
  "Legal" TEXT,
  "Acreage" REAL,
  "GPIN" INTEGER
);
.mode csv
.import real_estate.csv real_estate
EOF

ogr2ogr -F SQLite -dialect sqlite parcels.db parcels-projected.db -nln lots \
  -sql "$(cat <<EOF
SELECT
  PIN AS id,
  parcels.GPIN AS gpin,
  Geometry,
  ROUND(X(ST_Centroid(GeomFromWKB(Geometry))), 5) AS lon,
  ROUND(Y(ST_Centroid(GeomFromWKB(Geometry))), 5) AS lat,
  details.StreetNumber || ' ' || details.StreetName AS address,
  parcels,
  Acreage,
  Zone,
  0 AS tweeted
FROM ${source_table} parcels
JOIN (
  SELECT
    GPIN AS idx,
    COUNT(ParcelNumber) AS parcels,
    real_estate.*
  FROM real_estate
  GROUP BY GPIN
) details ON parcels.GPIN = details.GPIN
GROUP BY idx
EOF
)"
