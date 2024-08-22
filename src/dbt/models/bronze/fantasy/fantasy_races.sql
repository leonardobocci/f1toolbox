SELECT
    round_number,
    country_code,
    event_format,
    has_any_results AS has_fantasy_results,
    CAST(season AS Nullable (Int64)) AS season
FROM file('fantasy/races.parquet', 'Parquet')-- noqa: CP03
