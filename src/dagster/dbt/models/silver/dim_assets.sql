with

fantasy_constructors as (

    select * from {{ ref('fantasy_constructors') }}

),

fantasy_drivers as (

    select * from {{ ref('fantasy_drivers') }}

),

unioned as (
    select
        'Constructor' as asset_type,
        *
    from fantasy_constructors
    union all
    select
        'Driver' as asset_type,
        *
    from fantasy_drivers
),

fastf1_session_assets as (

    select distinct
        event_id,
        driver_number,
        driver_code,
        fastf1_driver_id,
        driver_first_name,
        driver_last_name,
        driver_full_name,
        driver_photo_url,
        driver_country_code,
        broadcast_name,
        fastf1_constructor_id,
        constructor_name,
        constructor_color

    from {{ ref('fastf1_results') }}

),

dim_assets as (
    --join on driver code and asset id
    select
        a.asset_type,
        a.asset_id,
        a.asset_name,
        b.event_id,
        b.driver_number,
        b.fastf1_driver_id,
        b.driver_first_name,
        b.driver_last_name,
        b.driver_full_name,
        b.driver_photo_url,
        b.driver_country_code,
        b.broadcast_name,
        b.fastf1_constructor_id,
        b.constructor_name,
        b.constructor_color
    from unioned as a
    left join fastf1_session_assets as b on a.asset_id = b.driver_code
)

select *
from dim_assets
