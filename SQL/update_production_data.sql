MERGE `propane-net-455409-s5.air_quality.production_data` AS target
USING (
  SELECT
    station_id,
    city,
    timestamp,
    aqi,
    pm25,
    pm10,
    temperature,
    humidity,
    latitude,
    longitude
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY station_id, timestamp) as rn
    FROM
      `propane-net-455409-s5.air_quality.raw_data`
  )
  WHERE rn = 1
) AS source
ON target.station_id = source.station_id AND target.timestamp = source.timestamp
WHEN NOT MATCHED THEN
  INSERT (station_id, city, timestamp, aqi, pm25, pm10, temperature, humidity, latitude, longitude)
  VALUES (source.station_id, source.city, source.timestamp, source.aqi, source.pm25, source.pm10, source.temperature, source.humidity, source.latitude, source.longitude)
WHEN MATCHED THEN
  UPDATE SET
    target.aqi = source.aqi,
    target.pm25 = source.pm25,
    target.pm10 = source.pm10,
    target.temperature = source.temperature,
    target.humidity = source.humidity,
    target.latitude = source.latitude,
    target.longitude = source.longitude;