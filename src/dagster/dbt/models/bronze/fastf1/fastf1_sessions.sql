SELECT
    id AS session_id,
    event_id,
    name AS session_name,
    type AS session_type,
    utc_start_datetime,
    utc_end_datetime
FROM file('fastf1/sessions.parquet', 'Parquet')
