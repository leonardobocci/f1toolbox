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
)

select *
from unioned
