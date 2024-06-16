with

circuit_corners as (

    select * from {{ ref('fastf1_circuit_corners') }}

),

grouped as (

    SELECT 
        season, circuit_key,
        COUNT(CASE WHEN turn_angle > 90 THEN 1 END) AS count_slow_corners,
        COUNT(CASE WHEN turn_angle BETWEEN 60 AND 90 THEN 1 END) AS count_medium_corners,
        COUNT(CASE WHEN turn_angle < 60 THEN 1 END) AS count_fast_corners,
        SUM(distance_from_last_corner) as straight_length,
        COUNT(CASE WHEN distance_from_last_corner < 300 THEN 1 END) as count_short_accelerations,
        COUNT(CASE WHEN distance_from_last_corner BETWEEN 300 AND 700 THEN 1 END) as count_medium_accelerations,
        COUNT(CASE WHEN distance_from_last_corner > 700 THEN 1 END) as count_long_accelerations
    FROM 
        circuit_corners
    GROUP BY 
        season, circuit_key

)

select * from grouped