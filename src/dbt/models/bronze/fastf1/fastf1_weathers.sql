SELECT
    timestamp AS session_timestamp, -- noqa: CP02
    end_timestamp AS valid_to,-- noqa: CP02
    AirTemp AS air_temperature, -- noqa: CP02
    Humidity AS humidity, -- noqa: CP02, AL09
    Pressure AS pressure, -- noqa: CP02, AL09
    Rainfall AS is_raining, -- noqa: CP02
    TrackTemp AS track_temperature, -- noqa: CP02
    WindDirection AS wind_direction_degrees, -- noqa: CP02
    WindSpeed AS wind_speed, -- noqa: CP02
    CAST(session_id AS Nullable(Int64)) AS session_id,-- noqa: CP02
    CAST(event_id AS Nullable(Int64)) AS event_id-- noqa: CP02
FROM file('fastf1/weathers.parquet', 'Parquet')-- noqa: CP01,CP03
