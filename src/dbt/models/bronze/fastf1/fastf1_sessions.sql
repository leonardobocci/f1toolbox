SELECT id as session_id, event_id, name as event_name, type as session_type, start_date, end_date, local_timezone_utc_offset
FROM file('fastf1/sessions.parquet', 'Parquet')
