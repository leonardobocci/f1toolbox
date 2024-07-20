with

fastf1_events as (

    select * from {{ ref('fastf1_events') }}

),

fastf1_sessions as (

    select * from {{ ref('fastf1_sessions') }}

),

fastf1_events_sessions as (

    select
        a.*,
        b.session_id,
        b.session_type,
        b.start_date,
        b.end_date,
        b.local_timezone_utc_offset
    from fastf1_events as a
    inner join
        fastf1_sessions as b
        on a.event_id = b.event_id
),

fastf1_weathers as (

    select * from {{ ref('fastf1_weathers') }}

),

session_weathers as (

    select
        session_id,
        event_id,
        avg(air_temperature) as avg_air_temperature,
        avg(track_temperature) as avg_track_temperature,
        avg(humidity) as avg_humidity,
        avg(wind_speed) as avg_wind_speed,
        sum(is_raining)
        / count(is_raining) as raining_percentage_of_session_time
    from
        fastf1_weathers
    group by
        session_id,
        event_id
),

fastf1_events_sessions_weathers as (

    select
        a.*,
        b.avg_air_temperature,
        b.avg_track_temperature,
        b.avg_humidity,
        b.avg_wind_speed,
        b.raining_percentage_of_session_time
    from fastf1_events_sessions as a
    inner join
        session_weathers as b
        on
            a.session_id = b.session_id
            and a.event_id = b.event_id

),

fantasy_races as (

    select * from {{ ref('fantasy_races') }}

),

fastf1_joined_fantasy as (
    select
        a.*,
        b.event_format,
        b.has_fantasy_results
    from fastf1_events_sessions_weathers as a
    left join
        fantasy_races as b
        on a.round_number = b.round_number and a.season = b.season
),

dim_circuits as (

    select * from {{ ref('dim_circuits') }}

),

dim_track_sessions as (

    select
        a.*,
        b.count_slow_corners,
        b.count_medium_corners,
        b.count_fast_corners,
        b.straight_length,
        b.count_short_accelerations,
        b.count_medium_accelerations,
        b.count_long_accelerations
    from fastf1_joined_fantasy as a
    inner join
        dim_circuits as b
        on a.circuit_key = b.circuit_key and a.season = b.season

)

select * from dim_track_sessions
