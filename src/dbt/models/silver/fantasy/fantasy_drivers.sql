{{ config(group = 'silver_fantasy_views') }}
SELECT * FROM file('fantasy/drivers.parquet', 'Parquet')
