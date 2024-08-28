SELECT
    compound,
    color,
    season
FROM
    file('fastf1/tyre_compounds.parquet', 'Parquet')
