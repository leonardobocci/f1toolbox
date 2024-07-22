with

timed_laps as (

    select
        session_id,
        event_id,
        constructor_name,
        driver_code,
        driver_number,
        stint_number,
        session_time_lap_end,
        lap_time,
        tyre_compound,
        tyre_age_laps
    from {{ ref('fastf1_laps') }}
    where pit_out_time is null and pit_in_time is null and track_status = '1'

),

best_laps_on_tyre as (

    select
        session_id,
        driver_code,
        tyre_compound,
        min(lap_time) as best_lap_time_on_tyre
    from {{ ref('fastf1_laps') }}
    group by session_id, driver_code, tyre_compound
),

joined as (
    select
        a.*,
        b.best_lap_time_on_tyre
    from timed_laps as a
    left join
        best_laps_on_tyre as b
        on
            a.session_id = b.session_id
            and a.driver_code = b.driver_code
            and a.tyre_compound = b.tyre_compound
),

stint_progression as (
    select
        *,
        ((lap_time / best_lap_time_on_tyre) - 1) as pct_away_from_best_lap
    from joined
)

select * from stint_progression
