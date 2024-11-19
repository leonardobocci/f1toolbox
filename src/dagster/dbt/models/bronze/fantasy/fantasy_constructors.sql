SELECT
    id AS asset_id,
    name AS asset_name
FROM
    {{ source("f1toolbox_core", "bq_bronze_fantasy_current_constructors") }}
