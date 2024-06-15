with

constructor_scores as (

    select id, season, round_number, price, price_change, points_scored from {{ ref('fantasy_constructor_attributes') }}

),

driver_scores as (

    select id, season, round_number, price, price_change, points_scored from {{ ref('fantasy_driver_attributes') }}

),

unioned as (
    SELECT 'Constructor' as asset_type,
        * from constructor_scores
    UNION ALL
    SELECT 'Driver' as asset_type,
        * from driver_scores
),

calculated as (
    SELECT *, points_scored/price as points_per_price FROM unioned WHERE points_scored IS NOT NULL
),

ranked as (
    SELECT *,
        rank() over (partition by asset_type, season, round_number order by points_per_price desc) as rank_value,
        rank() over (partition by asset_type, season, round_number order by points_scored desc) as rank_points
    FROM calculated
)

select * from ranked
