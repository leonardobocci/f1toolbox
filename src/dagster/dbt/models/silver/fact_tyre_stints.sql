with

/*first select all the laps where
 the driver is not entering
 or exiting the pits
 and track has green flag
*/
valid_laps as (

    select
        session_id,
        event_id,
        constructor_name,
        driver_code,
        driver_number,
        stint_number,
        lap_start_timestamp,
        lap_time,
        tyre_compound,
        tyre_age_laps
    from {{ ref('fastf1_laps') }}
    where
        pit_out_time is null
        and pit_in_time is null
        and track_status = '1'
),

--aggregate the lap times for each stint
car_tyre_stint_times as (

    select
        session_id,
        event_id,
        constructor_name,
        driver_code,
        driver_number,
        stint_number,
        tyre_compound,
        sum(lap_time) as total_stint_time,
        count(tyre_age_laps) as stint_length
    from valid_laps
    group by
        session_id,
        event_id,
        constructor_name,
        driver_code,
        driver_number,
        stint_number,
        tyre_compound
)

select * from car_tyre_stint_times
