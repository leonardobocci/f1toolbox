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
    from timed_laps
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
),

regression as (
    select
        session_id,
        event_id,
        constructor_name,
        driver_code,
        tyre_compound,
        count(*) as n,
        sum(tyre_age_laps) as sum_x,
        sum(pow(tyre_age_laps, 2)) as sum_of_squares,
        sum(pct_away_from_best_lap) as sum_y,
        sum(tyre_age_laps * pct_away_from_best_lap) as sum_xy
    from stint_progression
    group by
        session_id,
        event_id,
        constructor_name,
        driver_code,
        tyre_compound
),

tyre_age_factor as (
    /*Calculate the slopes (factor) of gap from best lap depending on tyre age,
    for each session, driver, constructor, compound*/
    select
        session_id,
        event_id,
        constructor_name,
        driver_code,
        tyre_compound,
        (n * sum_xy - sum_x * sum_y)
        / (n * sum_of_squares - sum_x * sum_x) as tyre_age_factor
    from regression
)

select * from tyre_age_factor
