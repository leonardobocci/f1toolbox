
SELECT CAST(season AS Nullable(Int64)) as season,round_number,country_code,event_format,has_any_results as has_fantasy_results FROM file('fantasy/races.parquet', 'Parquet')
