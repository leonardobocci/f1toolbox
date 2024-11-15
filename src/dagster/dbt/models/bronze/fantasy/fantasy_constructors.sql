SELECT
    id AS asset_id,
    name AS asset_name,
    active AS is_active,
    last_updated
FROM
    {{ source("dagster", "bq_bronze_fantasy_current_constructors") }}
