SELECT
    car_number,
    Date AS telemetry_datetime,-- noqa: CP02
    RPM AS rpm, -- noqa: CP02, AL09
    Speed AS speed, -- noqa: CP02, AL09
    nGear AS gear_number, -- noqa: CP02
    Throttle AS throttle_percentage, -- noqa: CP02
    Brake AS is_braking,-- noqa: CP02
    Source AS telemetry_source,-- noqa: CP02
    Time AS time_from_lap_start,-- noqa: CP02
    SessionTime AS time_from_session_start, -- noqa: CP02
    Status AS on_track_status, -- noqa: CP02
    X AS x_coordinate, -- noqa: CP02
    Y AS y_coordinate, -- noqa: CP02
    Z AS z_coordinate, -- noqa: CP02
    CAST(session_id AS Nullable(Int64)) AS session_id,
    CAST(DRS AS Nullable(Bool)) AS is_drs_enabled -- noqa: CP02
FROM file('fastf1/telemetry.parquet', 'Parquet')-- noqa: CP03
