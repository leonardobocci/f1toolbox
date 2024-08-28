with

constructor_scores as (

    select
        result_id,
        season,
        round_number,
        price,
        price_change,
        points_scored
    from {{ ref('fantasy_constructor_attributes') }}

),

driver_scores as (

    select
        result_id,
        season,
        round_number,
        price,
        price_change,
        points_scored
    from {{ ref('fantasy_driver_attributes') }}

),

unioned as (
    select
        'Constructor' as asset_type,
        *
    from constructor_scores
    union all
    select
        'Driver' as asset_type,
        *
    from driver_scores
),

calculated as (
    select
        *,
        points_scored / price as points_per_price
    from unioned
    where points_scored is not NULL
),

ranked as (
    select
        *,
        rank()
            over (
                partition by asset_type, season, round_number
                order by points_per_price desc
            )
            as rank_value,
        rank()
            over (
                partition by asset_type, season, round_number
                order by points_scored desc
            )
            as rank_points
    from calculated
)

select * from ranked
