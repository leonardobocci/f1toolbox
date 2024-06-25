CREATE DATABASE IF NOT EXISTS f1;

CREATE TABLE f1.fastf1_events
(
    id UInt32,
    name String,
    Country_Key UInt16,
    Country_Code String,
    Country_Name String,
    Circuit_Key UInt16,
    Circuit_ShortName String,
)
ENGINE = ReplacingMergeTree
PRIMARY KEY (id);

CREATE TABLE f1.fastf1_laps
(
    Time UInt64,
    Driver String,
    DriverNumber UInt8,
    LapTime UInt64,
    LapNumber UInt8,
    Stint UInt8,
    PitOutTime UInt64,
    PitInTime UInt64,
    Sector1Time UInt64,
    Sector2Time UInt64,
    Sector3Time UInt64,
    Sector1SessionTime UInt64,
    Sector2SessionTime UInt64,
    Sector3SessionTime UInt64,
    Speed1 UInt16,
    Speed2 UInt16,
    SpeedFL UInt16,
    SpeedST UInt16,
    IsPersonalBest Bool,
    Compound String,
    TyreLife UInt8,
    FreshTyre Bool,
    Team String,
    LapStartTime UInt64,
    LapStartDate DateTime,
    TrackStatus String,
    Position UInt8,
    Deleted String,
    DeletedReason String,
    FastF1Generated Bool,
    IsAccurate Bool,
    session_id UInt32,
    event_id UInt32,
)
ENGINE = ReplacingMergeTree
PRIMARY KEY (session_id, event_id, LapNumber, Driver);

CREATE TABLE f1.fastf1_results
(
    DriverNumber UInt8,
    BroadcastName String,
    Abbreviation String,
    DriverId String,
    TeamName String,
    TeamColor String,
    TeamId String,
    FirstName String,
    LastName String,
    FullName String,
    HeadshotUrl String,
    CountryCode String,
    Position UInt8,
    ClassifiedPosition String,
    GridPosition UInt8,
    Q1 UInt32,
    Q2 UInt32,
    Q3 UInt32,
    Time UInt64,
    Status String,
    Points UInt8,
    session_id UInt32,
    event_id UInt32,
)
ENGINE = ReplacingMergeTree
PRIMARY KEY (session_id, event_id, DriverNumber);

CREATE TABLE f1.fastf1_sessions
(
    id UInt32,
    event_id UInt32,
    name String,
    type String,
    start_date DateTime,
    end_date DateTime,
    local_timezone_utc_offset String,
)
ENGINE = ReplacingMergeTree
PRIMARY KEY (id);

CREATE TABLE f1.fastf1_telemetry
(
    Date DateTime,
    RPM UInt16,
    Speed UInt16,
    nGear UInt8,
    Throttle UInt8,
    Brake Bool,
    DRS UInt8,
    Source String,
    Time UInt64,
    SessionTime UInt64,
    Status String,
    X Int64,
    Y Int64,
    Z Int64,
    car_number UInt8,
    session_id UInt32,
)
ENGINE = ReplacingMergeTree
PRIMARY KEY (session_id, SessionTime, car_number);

CREATE TABLE f1.fastf1_weathers
(
    Time UInt64,
    AirTemp Int16,
    Humidity UInt8,
    Pressure UInt16,
    Rainfall Bool,
    TrackTemp Int16,
    WindDirection UInt16,
    WindSpeed UInt16,
    session_id UInt32,
    event_id UInt32,
)
ENGINE = ReplacingMergeTree
PRIMARY KEY (session_id, event_id, Time);

CREATE TABLE f1.fantasy_constructor_attributes
(
    points_scored Int16,
    id String,
    color String,
    sprint_pos_gained_points Int16,
    sprint_overtake_points Int16,
    sprint_fastest_lap_points Int16,
    sprint_not_classified_points Int16,
    sprint_disqualified_points Int16,
    sprint_pos_points Int16,
    quali_not_classified_points Int16,
    quali_disqualified_points Int16,
    quali_pos_points Int16,
    quali_teamwork_points Int16,
    race_pos_gained_points Int16,
    race_overtake_points Int16,
    race_fastest_lap_points Int16,
    race_not_classified_points Int16,
    race_disqualified_points Int16,
    race_pos_points Int16,
    race_pit_stop_points Int16,
    price UInt16,
    price_change Int16,
    season UInt16,
    round_number UInt16,
)
ENGINE = ReplacingMergeTree
PRIMARY KEY (id, season, round_number);

CREATE TABLE f1.fantasy_driver_attributes
(
    points_scored Int16,
    id String,
    color String,
    sprint_pos_gained_points Int16,
    sprint_overtake_points Int16,
    sprint_fastest_lap_points Int16,
    sprint_not_classified_points Int16,
    sprint_disqualified_points Int16,
    sprint_pos_points Int16,
    quali_not_classified_points Int16,
    quali_disqualified_points Int16,
    quali_pos_points Int16,
    quali_teamwork_points Int16,
    race_pos_gained_points Int16,
    race_overtake_points Int16,
    race_fastest_lap_points Int16,
    race_not_classified_points Int16,
    race_disqualified_points Int16,
    race_pos_points Int16,
    race_pit_stop_points Int16,
    price UInt16,
    price_change Int16,
    season UInt16,
    round_number UInt16,
)
ENGINE = ReplacingMergeTree
PRIMARY KEY (id, season, round_number);

CREATE TABLE f1.fantasy_constructors
(
    name String,
    active Bool,
    id String,
    color String,
    last_updated DateTime,
)
ENGINE = ReplacingMergeTree
PRIMARY KEY (id);

CREATE TABLE f1.fantasy_drivers
(
    name String,
    active Bool,
    id String,
    color String,
    last_updated DateTime,
)
ENGINE = ReplacingMergeTree
PRIMARY KEY (id);

CREATE TABLE f1.fantasy_races
(
    country_code String,
    event_format String,
    has_any_results Bool,
    round_number UInt16,
    season UInt16,
)
ENGINE = ReplacingMergeTree
PRIMARY KEY (season, round_number);
