SELECT session_id, event_id, Time as session_timestamp, AirTemp as air_temperature, Humidity as humidity, Pressure as pressure, Rainfall as is_raining, TrackTemp as track_temperature, WindDirection as wind_direction_degrees, WindSpeed as wind_speed
FROM file('fastf1/weathers.parquet', 'Parquet')
