#!/bin/bash

set -euo pipefail

rm -f parcels.db

# https://opendata.dc.gov/datasets/metro-bus-stops
curl -o stops.csv https://opendata.arcgis.com/datasets/e85b5321a5a84ff9af56fd614dab81b3_53.csv

sqlite3 parcels.db <<EOF
.import --csv stops.csv stops

CREATE TABLE lots AS
SELECT
  REG_ID AS id,
  BSTP_MSG_TEXT AS address,
  CAST(BSTP_LAT AS FLOAT) AS lat,
  CAST(BSTP_LON AS FLOAT) AS lon,
  0 AS tweeted
FROM stops
EOF
