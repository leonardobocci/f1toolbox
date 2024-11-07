SELECT
    id AS session_id,
    session_number,
    event_id,
    name AS session_name,
    type AS session_type,
    utc_start_datetime,
    utc_end_datetime
FROM file('fastf1/sessions.parquet', 'Parquet')
