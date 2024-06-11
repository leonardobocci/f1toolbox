{{ config(group = 'silver_fantasy_views') }}
SELECT * FROM file('fantasy/races.parquet', 'Parquet')
