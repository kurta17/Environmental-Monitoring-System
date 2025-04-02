CREATE TABLE `propane-net-455409-s5.air_quality.production_data`
(
  station_id INTEGER,
  city STRING,
  timestamp TIMESTAMP,
  aqi INTEGER,
  pm25 FLOAT64,
  pm10 FLOAT64,
  temperature FLOAT64,
  humidity FLOAT64,
  latitude FLOAT64,
  longitude FLOAT64
)
PARTITION BY DATE(timestamp)
OPTIONS (
  description = "Production table for air quality data, updated from raw_data without duplicates"
);