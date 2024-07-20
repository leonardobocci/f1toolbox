SELECT
    year AS season,
    circuit_key,
    x AS x_coordinate,
    y AS y_coordinate,
    number AS turn_number,
    angle AS turn_angle,
    distance_from_last_corner
FROM file('fastf1/circuit_corners.parquet', 'Parquet')
