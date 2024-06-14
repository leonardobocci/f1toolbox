SELECT CAST(session_id AS Nullable(Int64)) as session_id, CAST(event_id AS Nullable(Int64)) as event_id, Time as session_timestamp, AirTemp as air_temperature, Humidity as humidity, Pressure as pressure, Rainfall as is_raining, TrackTemp as track_temperature, WindDirection as wind_direction_degrees, WindSpeed as wind_speed
FROM file('fastf1/weathers.parquet', 'Parquet')
