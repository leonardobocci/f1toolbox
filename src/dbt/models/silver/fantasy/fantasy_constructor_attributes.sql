{{ config(group = 'silver_fantasy_views') }}
--SELECT * FROM file('fantasy/constructor_fantasy_attributes.parquet', 'Parquet')
SELECT * FROM {{ source("dagster", "bronze_fantasy_constructor_results") }}
