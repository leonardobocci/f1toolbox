SELECT
    compound,
    color,
    season
FROM
    {{ source("dagster", "bq_bronze_fastf1_tyres") }}
