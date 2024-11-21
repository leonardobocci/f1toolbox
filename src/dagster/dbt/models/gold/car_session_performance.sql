--TODO: ADJUST using tyre_event_performance age and tyre gap factors

with

accelerations as (select * from {{ ref('fact_accelerations') }}),

slow_corners as (
    select
        session_id,
        car_number,
        event_id,
        avg(lateral_acceleration) as slow_corners_avg_lateral_acceleration
    from accelerations
    where corner_status = 'SLOW_CORNER'
    group by session_id, car_number, event_id
),

medium_corners as (
    select
        session_id,
        car_number,
        avg(lateral_acceleration) as medium_corners_avg_lateral_acceleration
    from accelerations
    where corner_status = 'MEDIUM_CORNER'
    group by session_id, car_number
),

fast_corners as (
    select
        session_id,
        car_number,
        avg(lateral_acceleration) as fast_corners_avg_lateral_acceleration
    from accelerations
    where corner_status = 'FAST_CORNER'
    group by session_id, car_number
),

lateral_averages as (
    select
        a.session_id,
        a.car_number,
        a.event_id,
        a.slow_corners_avg_lateral_acceleration,
        b.medium_corners_avg_lateral_acceleration,
        c.fast_corners_avg_lateral_acceleration
    from slow_corners as a
    inner join medium_corners as b
        on a.session_id = b.session_id and a.car_number = b.car_number
    inner join fast_corners as c
        on a.session_id = c.session_id and a.car_number = c.car_number
),

braking_zones as (
    select
        session_id,
        car_number,
        avg(longitudinal_acceleration)
            as braking_zones_avg_longitudinal_acceleration
    from accelerations
    where acceleration_status = 'BRAKING_ZONE'
    group by session_id, car_number
),

acceleration_zones as (
    select
        session_id,
        car_number,
        avg(longitudinal_acceleration)
            as acceleration_zones_avg_longitudinal_acceleration
    from accelerations
    where acceleration_status = 'ACCELERATION_ZONE'
    group by session_id, car_number
),

longitudinal_averages as (
    select
        a.session_id,
        a.car_number,
        a.braking_zones_avg_longitudinal_acceleration,
        b.acceleration_zones_avg_longitudinal_acceleration
    from braking_zones as a
    inner join acceleration_zones as b
        on a.session_id = b.session_id and a.car_number = b.car_number
),

max_speeds as (
    select
        session_id,
        car_number,
        max(speed) as max_speed
    from accelerations
    where corner_status = 'STRAIGHT'
    group by session_id, car_number
),

car_performance as (
    select
        a.session_id,
        a.car_number as left_car_number,
        a.event_id as left_event_id,
        a.slow_corners_avg_lateral_acceleration,
        a.medium_corners_avg_lateral_acceleration,
        a.fast_corners_avg_lateral_acceleration,
        b.braking_zones_avg_longitudinal_acceleration,
        b.acceleration_zones_avg_longitudinal_acceleration,
        c.max_speed
    from lateral_averages as a
    inner join
        longitudinal_averages as b
        on a.session_id = b.session_id and a.car_number = b.car_number
    inner join
        max_speeds as c
        on a.session_id = c.session_id and a.car_number = c.car_number
),

dim_assets as (select * from {{ ref('dim_assets') }}),

joined_car_performance as (
    select
        da.*,
        a.session_id,
        a.slow_corners_avg_lateral_acceleration,
        a.medium_corners_avg_lateral_acceleration,
        a.fast_corners_avg_lateral_acceleration,
        a.braking_zones_avg_longitudinal_acceleration,
        a.acceleration_zones_avg_longitudinal_acceleration,
        a.max_speed
    from car_performance as a
    inner join dim_assets as da
        on
            a.left_car_number = da.driver_number
            and a.left_event_id = da.event_id
),

dim_sessions as (select * from {{ ref('dim_sessions') }}),

rich_car_performance as (
    select
        a.*,
        b.season,
        b.round_number,
        b.event_name,
        b.country_key,
        b.circuit_key,
        b.country_code,
        b.circuit_shortname,
        b.session_type,
        b.session_number
    from joined_car_performance as a
    left join dim_sessions as b on a.session_id = b.session_id
)

select * from rich_car_performance
