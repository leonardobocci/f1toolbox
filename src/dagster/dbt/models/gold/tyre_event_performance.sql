with

granular_tyre_performance as (
    select *
    from {{ ref('fact_tyre_event_constructor') }}
),

aggregate_tyre_performance as (
    select
        event_id,
        tyre_compound,
        avg(avg_tyre_age_factor) as avg_tyre_age_factor,
        avg(pct_away_from_best_tyre) as avg_pct_away_from_best_tyre
    from granular_tyre_performance
    group by event_id, tyre_compound
),

dim_events as (
    select
        event_id,
        season,
        event_name,
        country_code,
        circuit_shortname
    from {{ ref('dim_events') }}
),

joined as (
    select
        a.event_id,
        a.tyre_compound,
        a.avg_tyre_age_factor,
        a.avg_pct_away_from_best_tyre,
        b.season,
        b.event_name,
        b.country_code,
        b.circuit_shortname
    from aggregate_tyre_performance as a
    inner join dim_events as b
        on a.event_id = b.event_id
)

select * from joined
