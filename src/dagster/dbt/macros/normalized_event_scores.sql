{% macro normalized_event_scores(constructor_name_or_driver_last_name) %}
    with

    car_session_performance as (
        select * from {{ ref('car_session_performance') }}
    ),

    asset_event_performance as (
        select
            {{ constructor_name_or_driver_last_name }},
            event_id,
            season,
            round_number,
            event_name,
            avg(slow_corners_avg_lateral_acceleration)
                as slow_corners_avg_lateral_acceleration,
            avg(medium_corners_avg_lateral_acceleration)
                as medium_corners_avg_lateral_acceleration,
            avg(fast_corners_avg_lateral_acceleration)
                as fast_corners_avg_lateral_acceleration,
            avg(braking_zones_avg_longitudinal_acceleration)
                as braking_zones_avg_longitudinal_acceleration,
            avg(acceleration_zones_avg_longitudinal_acceleration)
                as acceleration_zones_avg_longitudinal_acceleration,
            avg(max_speed) as avg_top_speed
        from car_session_performance
        group by {{ constructor_name_or_driver_last_name }}, event_id,season, round_number, event_name
    ),

    metrics_min_max as (
        select
            event_id,
            min(slow_corners_avg_lateral_acceleration) as min_slow_corners,
            max(slow_corners_avg_lateral_acceleration) as max_slow_corners,
            min(medium_corners_avg_lateral_acceleration) as min_medium_corners,
            max(medium_corners_avg_lateral_acceleration) as max_medium_corners,
            min(fast_corners_avg_lateral_acceleration) as min_fast_corners,
            max(fast_corners_avg_lateral_acceleration) as max_fast_corners,
            min(braking_zones_avg_longitudinal_acceleration) as min_braking_zones,
            max(braking_zones_avg_longitudinal_acceleration) as max_braking_zones,
            min(acceleration_zones_avg_longitudinal_acceleration)
                as min_acceleration_zones,
            max(acceleration_zones_avg_longitudinal_acceleration)
                as max_acceleration_zones,
            min(avg_top_speed) as min_top_speed,
            max(avg_top_speed) as max_top_speed
        from asset_event_performance
        group by event_id
    ),

    normalized_scores as (
        select
            a.{{ constructor_name_or_driver_last_name }},
            a.event_id,
            a.season,
            a.round_number,
            a.event_name,
            10
            * safe_divide(
                (a.slow_corners_avg_lateral_acceleration - b.min_slow_corners),
                (b.max_slow_corners - b.min_slow_corners)
            ) as normalized_slow_corners,
            10
            * safe_divide(
                (a.medium_corners_avg_lateral_acceleration - b.min_medium_corners),
                (b.max_medium_corners - b.min_medium_corners))
                as normalized_medium_corners,
            10
            * safe_divide(
                (a.fast_corners_avg_lateral_acceleration - b.min_fast_corners),
                (b.max_fast_corners - b.min_fast_corners)
            ) as normalized_fast_corners,
            10
            * safe_divide(
                (a.braking_zones_avg_longitudinal_acceleration - b.min_braking_zones),
                (b.max_braking_zones - b.min_braking_zones)
            ) as normalized_braking_zones,
            10
            * safe_divide((
                a.acceleration_zones_avg_longitudinal_acceleration
                - b.min_acceleration_zones
            ),
            (b.max_acceleration_zones - b.min_acceleration_zones))
                as normalized_acceleration_zones,
            10
            * safe_divide(
                (a.avg_top_speed - b.min_top_speed),
                (b.max_top_speed - b.min_top_speed)
            ) as normalized_top_speed
        from
            asset_event_performance as a
        inner join
            metrics_min_max as b
            on
                a.event_id = b.event_id
    ),

    aggregate_normalized_scores as (
        select
            *,
            (normalized_slow_corners + normalized_medium_corners + normalized_fast_corners) / 3
                as avg_cornering_score,
            (normalized_acceleration_zones + normalized_braking_zones + normalized_top_speed) / 3
                as longitudinal_forces_score,
        from normalized_scores
    )

    select * from aggregate_normalized_scores
{% endmacro %}
