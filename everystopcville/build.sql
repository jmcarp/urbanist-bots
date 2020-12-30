CREATE TABLE stops (
  "x" FLOAT,
  "y" FLOAT,
  "object_id" INTEGER,
  "stop_id" INTEGER,
  "stop_code" INTEGER,
  "stop_name" TEXT,
  "latitude" FLOAT,
  "longitude" FLOAT,
  "stop_url" TEXT
);

.import --csv --skip 1 stops.csv stops

CREATE TABLE counts_raw (
  "transit_id" INTEGER,
  "stop" TEXT,
  "route" TEXT,
  "date_time" TEXT,
  "count" INT,
  "fare" FLOAT,
  "fare_category" TEXT,
  "payment_type" TEXT,
  "latitude" FLOAT,
  "longitude" FLOAT
);

.import --csv --skip 1 counts.csv counts_raw

CREATE TABLE counts AS
SELECT
  transit_id,
  TRIM(SUBSTR(stop, 0, INSTR(stop, '-'))) AS stop_code,
  TRIM(SUBSTR(stop, INSTR(stop, '-') + 1)) AS stop_name,
  route,
  DATETIME(REPLACE(REPLACE(date_time, '/', '-'), '+00', '')) AS timestamp,
  count,
  fare,
  fare_category,
  payment_type,
  latitude,
  longitude
FROM counts_raw
WHERE stop != 'Please refer to the Latitude/Longitude for location'
;

CREATE TABLE stop_routes AS
WITH pairs AS (
  SELECT DISTINCT
    stop_code,
    route
  FROM counts
  WHERE route != '0'
)
SELECT
  stop_code,
  GROUP_CONCAT(route, ', ') AS routes
FROM pairs
GROUP BY stop_code
;

CREATE TABLE stop_counts AS
WITH date_counts AS (
  SELECT
    stop_code,
    DATE(timestamp) AS date, 
    SUM(count) AS count
  FROM counts
  GROUP BY stop_code, DATE(timestamp)
)
SELECT
  stop_code, 
  AVG(count) AS count 
FROM date_counts
GROUP BY stop_code
;

CREATE TABLE lots AS
SELECT
  stop_id as id,
  stop_name as address,
  latitude as lat,
  longitude as lon,
  COALESCE(stop_routes.routes, '?') AS routes,
  CASE
    WHEN stop_counts.count IS NOT NULL THEN PRINTF('%.2f', stop_counts.count)
    ELSE '?'
  END AS count,
  0 AS tweeted
FROM stops
LEFT JOIN stop_routes USING (stop_code)
LEFT JOIN stop_counts USING (stop_code)
;
