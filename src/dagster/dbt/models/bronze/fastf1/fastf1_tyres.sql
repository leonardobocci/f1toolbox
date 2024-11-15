SELECT
    compound,
    color,
    season
FROM
    {{ source("f1toolbox_core", "bq_bronze_fastf1_tyres") }}
