SELECT
    id AS asset_id,
    name AS asset_name,
    active AS is_active,
    _ab_source_file_last_modified AS last_updated
FROM
    {{ source("f1toolbox_core", "bq_bronze_fantasy_current_drivers") }}
