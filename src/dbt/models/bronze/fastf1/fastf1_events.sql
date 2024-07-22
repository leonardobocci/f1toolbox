SELECT
    id AS event_id,
    name AS event_name,
    Country_Key AS country_key,-- noqa: CP02, AL09
    Country_Code AS country_code,-- noqa: CP02, AL09
    Circuit_Key AS circuit_key,-- noqa: CP02, AL09
    Circuit_ShortName AS circuit_shortname,-- noqa: CP02, AL09
    season,
    round_number,
    session_number
FROM
    file('fastf1/events.parquet', 'Parquet')
