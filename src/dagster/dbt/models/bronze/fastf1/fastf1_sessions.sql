SELECT
    id AS session_id,
    session_number,
    event_id,
    name AS session_name,
    utc_start_datetime, --bigquery was reading as json
    utc_end_datetime,
    CAST(type AS STRING) AS session_type
FROM {{ source("f1toolbox_core", "bq_bronze_fastf1_sessions") }}
