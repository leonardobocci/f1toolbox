with

fantasy_races as (

    select season,round_number,country_code,event_format,has_fantasy_results from {{ ref('fantasy_races') }}

),

fastf1_events as (

    select event_id,season,round_number,session_number,event_name,country_key,country_code, circuit_key, circuit_shortname from {{ ref('fastf1_events') }}

),

joined as (
    select event_id,a.season,a.round_number,session_number,event_name,country_key,a.country_code,circuit_key,circuit_shortname, event_format,has_fantasy_results
    from fastf1_events a
    left join fantasy_races b on a.round_number = b.round_number and a.season = b.season   
)

select * from joined