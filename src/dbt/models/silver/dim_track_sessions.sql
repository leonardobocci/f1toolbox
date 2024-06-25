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
        session_id, session_type, start_date, end_date, local_timezone_utc_offset
    from fastf1_events
    join fastf1_sessions on fastf1_events.event_id = fastf1_sessions.event_id
),

fastf1_weathers as (

    select * from {{ ref('fastf1_weathers') }}

),

session_weathers as (

    SELECT
        session_id,
        event_id,
        avg(air_temperature) AS avg_air_temperature,
        avg(track_temperature) AS avg_track_temperature,
        avg(humidity) AS avg_humidity,
        avg(wind_speed) AS avg_wind_speed,
        SUM(is_raining) / COUNT(is_raining) AS raining_percentage_of_session_time
    FROM
        fastf1_weathers
    GROUP BY
        session_id,
        event_id
),

fastf1_events_sessions_weathers as (

    select
        fastf1_events_sessions.*,
        avg_air_temperature, avg_track_temperature, avg_humidity, avg_wind_speed, raining_percentage_of_session_time
    from fastf1_events_sessions
    join session_weathers on fastf1_events_sessions.session_id = session_weathers.session_id and fastf1_events_sessions.event_id = session_weathers.event_id

),

fantasy_races as (

    select * from {{ ref('fantasy_races') }}

),

fastf1_joined_fantasy as (
    select
        a.*,
        event_format, has_fantasy_results
    from fastf1_events_sessions_weathers a
    left join fantasy_races b on a.round_number = b.round_number and a.season = b.season
),

dim_circuits as (

    select * from {{ ref('dim_circuits') }}

),

dim_track_sessions as (

    select
        a.*,
        count_slow_corners, count_medium_corners, count_fast_corners, straight_length, count_short_accelerations, count_medium_accelerations, count_long_accelerations
    from fastf1_joined_fantasy a
    join dim_circuits b on a.circuit_key = b.circuit_key and a.season = b.season

)

select * from dim_track_sessions
