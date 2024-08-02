SELECT
    DriverNumber AS driver_number,-- noqa: CP02
    Abbreviation AS driver_code,-- noqa: CP02
    BroadcastName AS broadcast_name,-- noqa: CP02
    DriverId AS fastf1_driver_id,-- noqa: CP02
    TeamName AS constructor_name,-- noqa: CP02
    TeamColor AS constructor_color, -- noqa: CP02
    TeamId AS fastf1_constructor_id, -- noqa: CP02
    FirstName AS driver_first_name,-- noqa: CP02
    LastName AS driver_last_name, -- noqa: CP02
    FullName AS driver_full_name, -- noqa: CP02
    HeadshotUrl AS driver_photo_url, -- noqa: CP02
    CountryCode AS driver_country_code, -- noqa: CP02
    Position AS current_position, -- noqa: CP02
    ClassifiedPosition AS classified_position,-- noqa: CP02
    GridPosition AS grid_position, -- noqa: CP02
    q1_lap_time_seconds,-- noqa: CP02
    q2_lap_time_seconds,-- noqa: CP02
    q3_lap_time_seconds,-- noqa: CP02
    lap_time_seconds,-- noqa: CP02
    Status AS fastf1_status, -- noqa: CP02
    Points AS championship_points, -- noqa: CP02
    CAST(session_id AS Nullable(Int64)) AS session_id,-- noqa: CP02
    CAST(event_id AS Nullable(Int64)) AS event_id-- noqa: CP02
FROM file('fastf1/results.parquet', 'Parquet')-- noqa: CP03
