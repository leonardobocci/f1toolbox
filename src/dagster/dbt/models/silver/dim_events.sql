with

fastf1_events as (

    select * from {{ ref('fastf1_events') }}

),

dim_circuits as (

    select * from {{ ref('dim_circuits') }}

),

dim_events as (

    select
        a.event_id,
        a.season,
        a.round_number,
        a.event_name,
        a.country_key,
        a.circuit_key,
        a.country_code,
        a.circuit_shortname,
        b.count_slow_corners,
        b.count_medium_corners,
        b.count_fast_corners,
        b.straight_length,
        b.count_short_accelerations,
        b.count_medium_accelerations,
        b.count_long_accelerations
    from fastf1_events as a
    inner join
        dim_circuits as b
        on a.circuit_key = b.circuit_key and a.season = b.season

)

select * from dim_events
