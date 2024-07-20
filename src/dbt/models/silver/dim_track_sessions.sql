with

fastf1_events as (

    select * from {{ ref('fastf1_events') }}

),

fastf1_sessions as (

    select * from {{ ref('fastf1_sessions') }}

),

fastf1_events_sessions as (

    select
        fastf1_events.*,
        session_id,
        session_type,
        start_date,
        end_date,
        local_timezone_utc_offset
    from fastf1_events
    inner join
        fastf1_sessions
        on fastf1_events.event_id = fastf1_sessions.event_id
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
        fastf1_events_sessions.*,
        avg_air_temperature,
        avg_track_temperature,
        avg_humidity,
        avg_wind_speed,
        raining_percentage_of_session_time
    from fastf1_events_sessions
    inner join
        session_weathers
        on
            fastf1_events_sessions.session_id = session_weathers.session_id
            and fastf1_events_sessions.event_id = session_weathers.event_id

),

fantasy_races as (

    select * from {{ ref('fantasy_races') }}

),

fastf1_joined_fantasy as (
    select
        a.*,
        event_format,
        has_fantasy_results
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
        count_slow_corners,
        count_medium_corners,
        count_fast_corners,
        straight_length,
        count_short_accelerations,
        count_medium_accelerations,
        count_long_accelerations
    from fastf1_joined_fantasy as a
    inner join
        dim_circuits as b
        on a.circuit_key = b.circuit_key and a.season = b.season

)

select * from dim_track_sessions
