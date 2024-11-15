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
    session_id,
    event_id
FROM {{ source("f1toolbox_core", "bq_bronze_fastf1_weathers") }}
