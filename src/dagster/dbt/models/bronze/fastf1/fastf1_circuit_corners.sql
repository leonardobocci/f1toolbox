SELECT
    year AS season,
    circuit_key,
    X AS x_coordinate, -- noqa: CP02
    Y AS y_coordinate, -- noqa: CP02
    Number AS turn_number, -- noqa: CP02
    Angle AS turn_angle, -- noqa: CP02
    distance_from_last_corner
FROM
    {{ source("f1toolbox_core", "bq_bronze_fastf1_circuit_corners") }}
