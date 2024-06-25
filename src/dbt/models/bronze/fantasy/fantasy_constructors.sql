
SELECT id as asset_id,name as asset_name,active as is_active,color,last_updated FROM file('fantasy/constructors.parquet', 'Parquet')
