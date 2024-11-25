with

event_car_tyre_performance as (
    select
        event_id,
        constructor_name,
        tyre_compound,
        safe_divide(total_stint_time, stint_length) as avg_tyre_lap_time
    from {{ ref('fact_tyre_stints') }}
),

best_tyre_performance as (
    select
        event_id,
        constructor_name,
        min(avg_tyre_lap_time) as best_tyre_performance
    from event_car_tyre_performance
    group by event_id, constructor_name
),

tyre_offsets as (
    select
        a.event_id,
        a.constructor_name,
        a.tyre_compound,
        a.avg_tyre_lap_time,
        b.best_tyre_performance,
        ((a.avg_tyre_lap_time / b.best_tyre_performance) - 1)
            as pct_away_from_best_tyre
    from event_car_tyre_performance as a
    left join best_tyre_performance as b
        on a.event_id = b.event_id and a.constructor_name = b.constructor_name
),

tyre_age_offsets as (
    select
        event_id,
        constructor_name,
        tyre_compound,
        avg(tyre_age_factor) as avg_tyre_age_factor
    from {{ ref('fact_tyre_aging') }}
    group by
        event_id, constructor_name, tyre_compound
),

tyre_performance as (
    select
        a.event_id,
        a.constructor_name,
        a.tyre_compound,
        a.avg_tyre_age_factor,
        b.pct_away_from_best_tyre
    from tyre_age_offsets as a
    inner join tyre_offsets as b
        on
            a.event_id = b.event_id
            and a.constructor_name = b.constructor_name
            and a.tyre_compound = b.tyre_compound
)

select * from tyre_performance
