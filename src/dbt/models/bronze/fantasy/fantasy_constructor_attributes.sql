SELECT
    id AS result_id,
    round_number,
    color,
    price,
    price_change,
    points_scored,
    sprint_pos_gained_points,
    sprint_overtake_points,
    sprint_fastest_lap_points,
    sprint_not_classified_points,
    sprint_disqualified_points,
    sprint_pos_points,
    quali_not_classified_points,
    quali_disqualified_points,
    quali_pos_points,
    quali_teamwork_points,
    race_pos_gained_points,
    race_overtake_points,
    race_fastest_lap_points,
    race_not_classified_points,
    race_disqualified_points,
    race_pos_points,
    race_pit_stop_points,
    NULL AS race_driver_of_day_points,
    CAST(season AS Nullable(Int64)) AS season
FROM FILE('fantasy/constructor_fantasy_attributes.parquet', 'Parquet')
