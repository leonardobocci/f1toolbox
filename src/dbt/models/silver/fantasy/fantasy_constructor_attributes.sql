{{ config(group = 'silver_fantasy_views') }}
SELECT * FROM file('fantasy/constructor_fantasy_attributes.parquet', 'Parquet')
