with

fantasy_constructors as (

    select * from {{ ref('fantasy_constructors') }}

),

fantasy_drivers as (

    select * from {{ ref('fantasy_drivers') }}

),

unioned as (
    SELECT 'Constructor' as asset_type,
        * from fantasy_constructors
    UNION ALL
    SELECT 'Driver' as asset_type,
        * from fantasy_drivers
)

SELECT * 
FROM unioned