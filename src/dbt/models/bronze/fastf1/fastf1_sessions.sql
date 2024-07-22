SELECT
    id AS session_id,
    event_id,
    name AS event_name,
    type AS session_type,
    start_date,
    end_date,
    local_timezone_utc_offset
FROM file('fastf1/sessions.parquet', 'Parquet')
