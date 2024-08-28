with

circuit_corners as (

    select * from {{ ref('fastf1_circuit_corners') }}

),

grouped as (

    select
        season,
        circuit_key,
        COUNT(case when turn_angle > 90 then 1 end) as count_slow_corners,
        COUNT(case when turn_angle between 60 and 90 then 1 end)
            as count_medium_corners,
        COUNT(case when turn_angle < 60 then 1 end) as count_fast_corners,
        SUM(distance_from_last_corner) as straight_length,
        COUNT(case when distance_from_last_corner < 300 then 1 end)
            as count_short_accelerations,
        COUNT(
            case when distance_from_last_corner between 300 and 700 then 1 end
        ) as count_medium_accelerations,
        COUNT(case when distance_from_last_corner > 700 then 1 end)
            as count_long_accelerations
    from
        circuit_corners
    group by
        season, circuit_key

)

select * from grouped
