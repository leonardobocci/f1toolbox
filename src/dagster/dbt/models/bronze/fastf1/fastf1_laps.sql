SELECT
    Driver AS driver_code,-- noqa: CP02
    Time AS session_time_lap_end,-- noqa: CP02
    end_time,-- noqa: CP02
    DriverNumber AS driver_number,-- noqa: CP02
    LapTime AS lap_time,-- noqa: CP02
    LapNumber AS lap_number,-- noqa: CP02
    Stint AS stint_number,-- noqa: CP02
    PitOutTime AS pit_out_time,-- noqa: CP02
    PitInTime AS pit_in_time,-- noqa: CP02
    Sector1Time AS sector_1_time,-- noqa: CP02
    Sector2Time AS sector_2_time,-- noqa: CP02
    Sector3Time AS sector_3_time,-- noqa: CP02
    Sector1SessionTime AS sector_1_session_time,-- noqa: CP02
    Sector2SessionTime AS sector_2_session_time,-- noqa: CP02
    Sector3SessionTime AS sector_3_session_time,-- noqa: CP02
    SpeedI1 AS speed_trap_1,-- noqa: CP02
    SpeedI2 AS speed_trap_2,-- noqa: CP02
    SpeedFL AS speed_trap_start_finish,-- noqa: CP02
    SpeedST AS speed_trap_longest_straight,-- noqa: CP02
    IsPersonalBest AS is_personal_best, -- noqa: CP02
    Compound AS tyre_compound, -- noqa: CP02
    TyreLife AS tyre_age_laps, -- noqa: CP02
    FreshTyre AS is_fresh_tyre, -- noqa: CP02
    Team AS constructor_name, -- noqa: CP02
    LapStartTime AS lap_start_time, -- noqa: CP02
    LapStartDate AS lap_start_date,-- noqa: CP02
    TrackStatus AS track_status,-- noqa: CP02
    Position AS current_position, -- noqa: CP02
    Deleted AS is_deleted,-- noqa: CP02
    DeletedReason AS deleted_reason,-- noqa: CP02
    FastF1Generated AS is_fastf1_generated,-- noqa: CP02
    IsAccurate AS is_accurate,-- noqa: CP02
    session_id,-- noqa: CP02
    event_id-- noqa: CP02
FROM
    {{ source("f1toolbox_core", "bq_bronze_fastf1_laps") }}
