SELECT year as season,circuit_key,X as x_coordinate,Y as y_coordinate,Number as turn_number,Angle as turn_angle,distance_from_last_corner FROM file('fastf1/circuit_corners.parquet', 'Parquet')
