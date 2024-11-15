SELECT
    round_number,
    country_code,
    event_format,
    has_any_results AS has_fantasy_results,
    season
FROM {{ source("dagster", "bq_bronze_fantasy_rounds") }}
