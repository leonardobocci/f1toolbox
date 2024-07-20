SELECT
    id AS event_id,
    name AS event_name,
    country_key,
    country_code,
    circuit_key,
    circuit_shortname,
    season,
    round_number,
    session_number
FROM file('fastf1/events.parquet', 'Parquet')
