/*TODO:
Due to clickhouse issue with join operator different than =
its not possible to align time frequency of weather data with lap data
*/
with

/*
weathers as (select * from {{ ref('fastf1_weathers') }}),

dry_tyre_stints as (

    select
        session_id,
        event_id,
        constructor_name,
        driver_code,
        driver_number,
        stint_number,
        session_time_lap_end,
        end_time,
        lap_time,
        tyre_compound,
        tyre_age_laps
    from {{ ref('fastf1_laps') }}
    where
        pit_out_time is null
        and pit_in_time is null
        and track_status = '1'
        and tyre_compound in (tuple('SOFT', 'MEDIUM', 'HARD'))

),

wet_tyre_stints as (

    select
        session_id,
        event_id,
        constructor_name,
        driver_code,
        driver_number,
        stint_number,
        session_time_lap_end,
        end_time,
        lap_time,
        tyre_compound,
        tyre_age_laps
    from {{ ref('fastf1_laps') }}
    where
        pit_out_time is null
        and pit_in_time is null
        and track_status = '1'
        and tyre_compound in (tuple('INTERMEDIATE', 'WET'))

),

dry_stints as (

    select
        a.*,
        b.session_timestamp,
        b.valid_to,
        b.is_raining
    from
        dry_tyre_stints as a
    left join
        weathers as b
        on
            a.session_id = b.session_id
            and a.session_time_lap_end >= b.session_timestamp
            and a.session_time_lap_end < b.valid_to
            and b.is_raining = 0
),

wet_stints as (

    select
        a.*,
        b.session_timestamp,
        b.valid_to,
        b.is_raining
    from wet_tyre_stints as a
    left join
        weathers as b
        on
            a.session_id = b.session_id
            and a.session_time_lap_end >= b.session_timestamp
            and a.session_time_lap_end < b.valid_to
            and b.is_raining = 1
),

all_weather_stints as (

    select
        session_id,
        event_id,
        constructor_name,
        driver_code,
        driver_number,
        stint_number,
        session_time_lap_end,
        end_time,
        lap_time,
        tyre_compound,
        tyre_age_laps,
        is_raining
    from dry_stints
    union all
    select
    session_id,
        event_id,
        constructor_name,
        driver_code,
        driver_number,
        stint_number,
        session_time_lap_end,
        end_time,
        lap_time,
        tyre_compound,
        tyre_age_laps,
        is_raining
    from wet_stints
),

*/

all_weather_stints as (
    select
        session_id,
        event_id,
        constructor_name,
        driver_code,
        driver_number,
        stint_number,
        session_time_lap_end,
        end_time,
        lap_time,
        tyre_compound,
        tyre_age_laps,
        0 as is_raining
    from {{ ref('fastf1_laps') }}
    where
        pit_out_time is null
        and pit_in_time is null
        and track_status = '1'
        and tyre_compound in (tuple('SOFT', 'MEDIUM', 'HARD'))
),

car_tyre_offset as (

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
    from all_weather_stints
    group by
        session_id,
        event_id,
        constructor_name,
        driver_code,
        driver_number,
        stint_number,
        tyre_compound
)

select * from car_tyre_offset
