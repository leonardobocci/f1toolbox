--fantasy tables
INSERT INTO f1.fantasy_races SELECT * FROM file('fantasy/races.parquet', 'Parquet');

INSERT INTO f1.fantasy_constructor_attributes SELECT * FROM file('fantasy/constructor_fantasy_attributes.parquet', 'Parquet');

INSERT INTO f1.fantasy_constructors SELECT * FROM file('fantasy/constructors.parquet', 'Parquet');

INSERT INTO f1.fantasy_driver_attributes SELECT * FROM file('fantasy/driver_fantasy_attributes.parquet', 'Parquet');

INSERT INTO f1.fantasy_drivers SELECT * FROM file('fantasy/drivers.parquet', 'Parquet');


--fastf1 tables
INSERT INTO f1.fastf1_events SELECT * FROM file('fastf1/events.parquet', 'Parquet');

INSERT INTO f1.fastf1_laps SELECT * FROM file('fastf1/laps.parquet', 'Parquet');

INSERT INTO f1.fastf1_results SELECT * FROM file('fastf1/results.parquet', 'Parquet');

INSERT INTO f1.fastf1_sessions SELECT * FROM file('fastf1/sessions.parquet', 'Parquet');

INSERT INTO f1.fastf1_telemetry SELECT * FROM file('fastf1/telemetry.parquet', 'Parquet');

INSERT INTO f1.fastf1_weathers SELECT * FROM file('fastf1/weathers.parquet', 'Parquet');