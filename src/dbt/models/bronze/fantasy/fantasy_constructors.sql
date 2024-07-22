SELECT
    id AS asset_id,
    name AS asset_name,
    active AS is_active,
    color,
    last_updated
FROM file('fantasy/constructors.parquet', 'Parquet')
