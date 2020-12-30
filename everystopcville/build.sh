#!/bin/bash

set -euo pipefail

rm -f parcels.db

# https://opendata.charlottesville.org/datasets/cat-bus-stop-points
curl -o stops.csv https://opendata.arcgis.com/datasets/6465cd54bcf4498495be8c86a9d7c3f2_4.csv

# https://opendata.charlottesville.org/datasets/transit-2020
curl -o counts.csv https://opendata.arcgis.com/datasets/f1cd175a268e46c9b8517dfe8e2fa931_29.csv

sqlite3 parcels.db < build.sql
