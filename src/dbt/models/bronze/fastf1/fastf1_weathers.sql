SELECT
    time AS session_timestamp,
    airtemp AS air_temperature,
    humidity,
    pressure,
    rainfall AS is_raining,
    tracktemp AS track_temperature,
    winddirection AS wind_direction_degrees,
    windspeed AS wind_speed,
    CAST(session_id AS Nullable(Int64)) AS session_id,
    CAST(event_id AS Nullable(Int64)) AS event_id
FROM FILE('fastf1/weathers.parquet', 'Parquet')
