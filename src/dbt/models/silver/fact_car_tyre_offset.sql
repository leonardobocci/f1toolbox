with

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
        and tyre_compound in ('SOFT', 'MEDIUM', 'HARD')

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
        and tyre_compound in ('INTERMEDIATE', 'WET')

),

dry_stints as (

    select a.*
    from
        dry_tyre_stints as a
    left join
        weathers as b
        on
            a.session_id = b.session_id
            and a.session_time_lap_end >= b.session_timestamp
            and a.session_time_lap_end < b.end_time
            and b.is_raining = 0
),

wet_stints as (

    select a.*
    from wet_tyre_stints as a
    left join
        weathers as b
        on
            a.session_id = b.session_id
            and a.session_time_lap_end >= b.session_timestamp
            and a.session_time_lap_end < b.end_time
            and b.is_raining = 1
),

all_weather_stints as (

    select * from dry_stints
    union all
    select * from wet_stints
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
        sum(lap_time) as total_stint_time
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
