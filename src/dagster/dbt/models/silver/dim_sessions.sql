with

dim_events as (

    select * from {{ ref('dim_events') }}

),

fastf1_sessions as (

    select * from {{ ref('fastf1_sessions') }}

),

dim_events_sessions as (

    select
        a.event_id,
        a.season,
        a.round_number,
        a.event_name,
        a.country_key,
        a.circuit_key,
        a.country_code,
        a.circuit_shortname,
        a.count_slow_corners,
        a.count_medium_corners,
        a.count_fast_corners,
        a.straight_length,
        a.count_short_accelerations,
        a.count_medium_accelerations,
        a.count_long_accelerations,
        b.session_id,
        b.session_number,
        b.session_name,
        b.session_type,
        b.utc_start_datetime,
        b.utc_end_datetime
    from dim_events as a
    inner join
        fastf1_sessions as b
        on a.event_id = b.event_id
),

weathers as (

    select
        *,
        cast(is_raining as INT64) as num_is_raining
    from {{ ref('fastf1_weathers') }}

),

session_weathers as (

    select
        session_id as right_session_id,
        event_id as right_event_id,
        avg(air_temperature) as avg_air_temperature,
        avg(track_temperature) as avg_track_temperature,
        avg(humidity) as avg_humidity,
        avg(wind_speed) as avg_wind_speed,
        sum(num_is_raining)
        / count(num_is_raining) as raining_percentage_of_session_time
    from
        weathers
    group by
        session_id,
        event_id
),

dim_events_sessions_weathers as (

    select
        a.*,
        b.avg_air_temperature,
        b.avg_track_temperature,
        b.avg_humidity,
        b.avg_wind_speed,
        b.raining_percentage_of_session_time
    from dim_events_sessions as a
    inner join
        session_weathers as b
        on
            a.session_id = b.right_session_id
            and a.event_id = b.right_event_id

),

fantasy_races as (

    select
        round_number as right_round_number,
        season as right_season,
        event_format,
        has_fantasy_results
    from {{ ref('fantasy_races') }}

),

fastf1_joined_fantasy as (
    select
        a.*,
        b.event_format,
        b.has_fantasy_results
    from dim_events_sessions_weathers as a
    left join
        fantasy_races as b
        on a.round_number = b.right_round_number and a.season = b.right_season
)

select * from fastf1_joined_fantasy
