#!/bin/bash

set -euo pipefail

rm -f parcels.db

# https://data.wprdc.org/dataset/port-authority-transit-stop-usage
curl -o stops.csv https://data.wprdc.org/dataset/ece64ad3-05eb-46dd-ba38-c83b5373812f/resource/3115c0b9-b48a-49aa-8e39-fd318eb62c04/download/busstopusagebyroute.csv

sqlite3 parcels.db <<EOF
.import --csv stops.csv stops

CREATE TABLE lots AS
WITH by_stop AS (
  SELECT
    STOP_ID,
    STOP_NAME,
    LATITUDE, LONGITUDE,
    ALL_ROUTES,
    SHELTER,
    ROW_NUMBER() OVER (PARTITION BY STOP_ID) AS idx
  FROM stops
)
SELECT
  STOP_ID AS id,
  STOP_NAME AS address,
  LATITUDE AS lat,
  LONGITUDE AS lon,
  ALL_ROUTES AS routes,
  SHELTER AS shelter,
  0 AS tweeted
FROM by_stop
WHERE idx = 1
;
EOF
