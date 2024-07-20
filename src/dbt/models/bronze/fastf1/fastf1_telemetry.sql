SELECT
    car_number,
    date AS telemetry_datetime,
    rpm,
    speed,
    ngear AS gear_number,
    throttle AS throttle_percentage,
    brake AS is_braking,
    source AS telemetry_source,
    time AS time_from_lap_start,
    sessiontime AS time_from_session_start,
    status AS on_track_status,
    x AS x_coordinate,
    y AS y_coordinate,
    z AS z_coordinate,
    CAST(session_id AS Nullable(Int64)) AS session_id,
    CAST(drs AS Nullable(Bool)) AS is_drs_enabled
FROM FILE('fastf1/telemetry.parquet', 'Parquet')
