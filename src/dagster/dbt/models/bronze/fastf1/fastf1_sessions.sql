SELECT
    id AS session_id,
    session_number,
    event_id,
    name AS session_name,
    type AS session_type,
    utc_start_datetime,
    utc_end_datetime
FROM {{ source("f1toolbox_core", "bq_bronze_fastf1_sessions") }}
