SELECT
    round_number,
    country_code,
    event_format,
    has_any_results AS has_fantasy_results,
    CAST(season AS INT64) AS season
FROM {{ source("f1toolbox_core", "bq_bronze_fantasy_rounds") }}
