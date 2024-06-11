{{ config(group = 'silver_fantasy_views') }}
SELECT * FROM file('fantasy/driver_fantasy_attributes.parquet', 'Parquet')
