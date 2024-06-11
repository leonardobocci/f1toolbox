{{ config(group = 'silver_fantasy_views') }}
SELECT * FROM file('fantasy/constructors.parquet', 'Parquet')
