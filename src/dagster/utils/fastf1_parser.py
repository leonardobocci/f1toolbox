from pathlib import Path

import numpy as np
import polars as pl
import scipy.signal as ss


def parse_parquet_signals(context, signal_directory: str) -> pl.LazyFrame:
    """Given an asset subdirectory, load all parquet files for all years and return a polars dataframe."""
    if signal_directory == "laps":
        df = pl.concat(
            [
                pl.scan_parquet(str(x)).with_columns(pl.col("Deleted").cast(pl.Utf8))
                for x in Path("data/landing/fastf1").rglob(
                    f"{signal_directory}/*.parquet"
                )
            ]
        )
    else:
        df = pl.scan_parquet(f"data/landing/fastf1/*/{signal_directory}/*.parquet")
    return df


def parse_session_timestamps(context, df: pl.LazyFrame) -> pl.DataFrame:
    df = df.with_columns(
        local_start_datetime=pl.col("start_date").str.to_datetime("%Y-%m-%dT%H:%M:%S"),
        local_end_datetime=pl.col("end_date").str.to_datetime("%Y-%m-%dT%H:%M:%S"),
    ).select(pl.exclude("start_date", "end_date"))
    df = df.with_columns(
        local_timezone_utc_offset=pl.col("local_timezone_utc_offset").str.split(",")
    )
    df = df.with_columns(
        pl.when(
            pl.col("local_timezone_utc_offset").list.len() == 2
        ).then(  # there are both day and hour offsets
            pl.col("local_timezone_utc_offset").list.first().alias("day_offset"),
        ),
        pl.when(pl.col("local_timezone_utc_offset").list.len() == 2)
        .then(
            pl.col("local_timezone_utc_offset").list.last(),
        )
        .otherwise(
            pl.when(
                pl.col("local_timezone_utc_offset").list.len() == 1
            ).then(  # there is only an hour offset
                pl.col("local_timezone_utc_offset").list.first(),
            ),
        )
        .alias("hour_offset"),
    ).select(pl.exclude("local_timezone_utc_offset"))
    df = df.with_columns(
        [
            pl.col("day_offset").str.split(" ").list.first() + "d",
            pl.col("hour_offset")
            .str.to_time()
            .cast(pl.Duration)
            .dt.total_seconds()
            .cast(pl.String)
            + "s",
        ]
    )  # prepare for offset by string processing function
    for mycol in ["local_start_datetime", "local_end_datetime"]:
        df = df.with_columns(
            pl.when(pl.col("day_offset").is_not_null())
            .then(pl.col(mycol).dt.offset_by(pl.col("day_offset")))
            .otherwise(pl.col(mycol))
        )
        df = df.with_columns(
            pl.when(pl.col("hour_offset").is_not_null())
            .then(pl.col(mycol).dt.offset_by(pl.col("hour_offset")))
            .otherwise(pl.col(mycol))
            .alias(f'utc_{mycol.removeprefix("local_")}')
        )
    df = df.select(
        pl.exclude(
            "hour_offset", "day_offset", "local_start_datetime", "local_end_datetime"
        )
    )
    return df.collect()  # tested as of polars 1.12.0
    # collect required to prevent polars.exceptions.ComputeError: unable to determine date parsing format, all values are null


def parse_results_lap_times(context, df: pl.LazyFrame) -> pl.LazyFrame:
    df = df.with_columns(
        lap_time_seconds=pl.col("Time") / 1e9,  # from nanoseconds to seconds
        q1_lap_time_seconds=pl.col("Q1") / 1e9,  # from nanoseconds to seconds
        q2_lap_time_seconds=pl.col("Q2") / 1e9,  # from nanoseconds to seconds
        q3_lap_time_seconds=pl.col("Q3") / 1e9,  # from nanoseconds to seconds
    )
    return df.select(pl.exclude("Time", "Q1", "Q2", "Q3"))


def parse_lap_timestamps(context, df: pl.LazyFrame) -> pl.LazyFrame:
    df = df.with_columns(
        session_time_lap_end=pl.col("Time"),
        lap_start_timestamp=pl.col("LapStartDate"),
        next_lap_start_timestamp=pl.col("LapStartDate").shift(-1),
    )
    return df.select(pl.exclude("Time", "LapStartDate"))


def parse_weather_timestamps(
    context, df: pl.LazyFrame, sessions_df: pl.LazyFrame
) -> pl.LazyFrame:
    """Given a weather dataframe, parse the timestamp and return a new column with the time in seconds from the start of the session."""
    selection = df.columns
    sessions_df = sessions_df.with_columns(id=pl.col("id").cast(pl.Int32))
    df = df.join(sessions_df, left_on="session_id", right_on="id")
    df = df.with_columns(
        seconds_from_session_start=pl.col("Time") / 1e9  # from nanoseconds to seconds
    )
    df = df.with_columns(
        timestamp=pl.col("seconds_from_session_start") + pl.col("utc_start_datetime"),
    )
    df = df.with_columns(
        end_timestamp=pl.col("timestamp").shift(
            -1, fill_value=pl.col("timestamp").last()
        )
    )
    df = df.select(*selection, "timestamp", "end_timestamp")
    df = df.select(pl.exclude("Time"))
    return df


def parse_telemetry_timestamps(context, df: pl.LazyFrame) -> pl.LazyFrame:
    """Get rid of fastf1 time column, leave only session time (time from start of the session) and date (utc datetime value)."""
    return df.select(pl.exclude("Time"))


def telemetry_coordinate_calculations(context, df: pl.LazyFrame) -> pl.LazyFrame:
    """Given a telemetry dataframe, calculate the previous two points, the time difference, and the speed difference."""
    telemetry = df.with_columns(
        pl.col("X").shift(1, fill_value=0).over("car_number").alias("x_prev_1"),
        pl.col("X").shift(2, fill_value=0).over("car_number").alias("x_prev_2"),
        pl.col("Y").shift(1, fill_value=0).over("car_number").alias("y_prev_1"),
        pl.col("Y").shift(2, fill_value=0).over("car_number").alias("y_prev_2"),
        pl.col("SessionTime").diff().alias("delta_time"),
        pl.col("Speed").diff().alias("delta_speed"),
    )
    return telemetry


def enrich_fastf1_telemetry(context, df: pl.LazyFrame) -> pl.LazyFrame:
    """Given three points, return the center of the circle that passes through them.
    https://stackoverflow.com/a/57406014.
    Given a point and the circle center, return the radius of the circle (euclidean distance).
    https://en.wikipedia.org/wiki/Euclidean_distance.
    Given each event's speed and radius, return the lateral acceleration of the car.
    https://www.mrwaynesclass.com/circular/notes/corner/home.htm"""

    def get_circle_center(context, df: pl.LazyFrame) -> pl.LazyFrame:
        temp_df = df.with_columns(
            [
                ((pl.col("x_prev_2") + pl.col("x_prev_1")) / 2).alias("ax"),
                ((pl.col("y_prev_2") + pl.col("y_prev_1")) / 2).alias("ay"),
                (pl.col("y_prev_2") - pl.col("y_prev_1")).alias("ux"),
                (pl.col("x_prev_1") - pl.col("x_prev_2")).alias("uy"),
                ((pl.col("x_prev_1") + pl.col("X")) / 2).alias("bx"),
                ((pl.col("y_prev_1") + pl.col("Y")) / 2).alias("by"),
                (pl.col("y_prev_1") - pl.col("Y")).alias("vx"),
                (pl.col("X") - pl.col("x_prev_1")).alias("vy"),
            ]
        )
        context.log.debug("Added required calculated fields for derivatives...")
        temp_df = temp_df.with_columns(
            [
                (pl.col("ax") - pl.col("bx")).alias("dx"),
                (pl.col("ay") - pl.col("by")).alias("dy"),
                (pl.col("vx") * pl.col("uy") - pl.col("vy") * pl.col("ux")).alias("vu"),
            ]
        )
        context.log.debug("Added first derivatives...")
        temp_df = temp_df.with_columns(
            (
                pl.when(pl.col("vu") == 0)
                .then(None)
                .otherwise(
                    (pl.col("dx") * pl.col("uy") - pl.col("dy") * pl.col("ux"))
                    / pl.col("vu")
                )
            ).alias("g")
        )
        context.log.debug("Added multiplication of first derivatives...")
        temp_df = temp_df.with_columns(
            [
                (pl.col("bx") + pl.col("g") * pl.col("vx")).alias("center_x"),
                (pl.col("by") + pl.col("g") * pl.col("vy")).alias("center_y"),
            ]
        )
        results = temp_df.select([*df.collect_schema().names(), "center_x", "center_y"])
        context.log.debug("Added circle center to each point...")
        return results

    def get_circle_radius(context, df: pl.LazyFrame) -> pl.LazyFrame:
        results = df.with_columns(
            (
                (pl.col("X") - pl.col("center_x")) ** 2
                + (pl.col("Y") - pl.col("center_y")) ** 2
            ).alias("squared_distance")
        )
        context.log.debug("Added square distance to center...")
        results = results.with_columns(
            pl.col("squared_distance").sqrt().alias("radius")
        )
        results = results.select(pl.exclude("squared_distance"))
        context.log.debug("Added circle radius...")
        return results

    def get_car_lateral_load(context, df: pl.LazyFrame) -> pl.LazyFrame:
        # convert to meters per second and meters
        # https://docs.fastf1.dev/core.html#telemetry for column units of measurements
        selection = df.collect_schema().names()
        df = df.with_columns(
            [
                (pl.col("Speed") / 3.6).alias("ms_speed"),
                (pl.col("radius") * 10).alias("m_radius"),
            ]
        )
        # acceleration in m/s^2
        df = df.with_columns(
            (pl.col("ms_speed").pow(2) / pl.col("m_radius")).alias(
                "lateral_acceleration"
            )
        )
        results = df.select([*selection, pl.col("lateral_acceleration")])
        # divide by 9.80665 to get g's if required
        context.log.debug("Added lateral acceleration.")
        return results

    def get_car_longitudinal_load(context, df: pl.LazyFrame) -> pl.LazyFrame:
        selection = df.collect_schema().names()
        df = df.with_columns(
            [
                (pl.col("delta_speed") / 3.6).alias("delta_ms_speed"),
                (pl.col("delta_time").dt.total_nanoseconds() / 1e9).alias(
                    "delta_s_time"
                ),  # convert from nanoseconds to seconds
            ]
        )
        df = df.with_columns(
            (pl.col("delta_ms_speed") / pl.col("delta_s_time")).alias(
                "longitudinal_acceleration"
            )
        )
        results = df.select([*selection, pl.col("longitudinal_acceleration")])
        # divide by 9.80665 to get g's if required.
        context.log.debug("Added longitudinal acceleration.")
        return results

    selection = df.collect_schema().names()
    df = get_circle_center(context, df)
    df = get_circle_radius(context, df)
    df = get_car_lateral_load(context, df)
    df = get_car_longitudinal_load(context, df)
    results = df.select(
        [*selection, "lateral_acceleration", "longitudinal_acceleration"]
    )
    results = results.select(pl.exclude("x_prev_1", "y_prev_1", "x_prev_2", "y_prev_2"))
    results = results.fill_nan(None)
    return results


def apply_digital_filter(
    context, df: pl.LazyFrame, filter_params: dict = {"window_len": 5, "polyorder": 3}
) -> pl.LazyFrame:
    """Apply a savitzky golay filter to the lateral and longitudinal acceleration columns.
    Use a window of 11 and a polynomial order of 5."""
    window_len = filter_params["window_len"]
    polyorder = filter_params["polyorder"]
    return df.with_columns(
        longitudinal_acceleration=pl.Series(
            ss.savgol_filter(
                np.ravel(df.select("longitudinal_acceleration").collect().to_numpy()),
                window_length=window_len,
                polyorder=polyorder,
            )
        ),
        lateral_acceleration=pl.Series(
            ss.savgol_filter(
                np.ravel(df.select("lateral_acceleration").collect().to_numpy()),
                window_length=window_len,
                polyorder=polyorder,
            )
        ),
    )


def handle_outliers(context, df: pl.LazyFrame, handling="drop") -> pl.LazyFrame:
    """
    Lateral max G's: over 5 according to Merc: https://www.mercedesamgf1.com/news/g-force-and-formula-one-explained
    lateral and braking up to 6, accelerating up to 4: https://f1chronicle.com/f1-g-force-how-many-gs-can-a-f1-car-pull/#G-Force-During-Acceleration
    """
    lower_bound_lateral = 0
    upper_bound_lateral = 58  # M/S^2, 6G's
    lower_bound_braking = -58  # M/S^2, 6G's
    upper_bound_acceleration = 39  # M/S^2, 4G's
    if handling == "drop":
        df = df.filter(
            (pl.col("lateral_acceleration") > lower_bound_lateral)
            & (pl.col("lateral_acceleration") < upper_bound_lateral)
            & (pl.col("longitudinal_acceleration") > lower_bound_braking)
            & (pl.col("longitudinal_acceleration") < upper_bound_acceleration)
        )
    elif handling == "winsorize":
        df = df.with_columns(
            pl.when(pl.col("lateral_acceleration") > upper_bound_lateral)
            .then(upper_bound_lateral)
            .otherwise(pl.col("lateral_acceleration"))
            .alias("lateral_acceleration"),
            pl.when(pl.col("longitudinal_acceleration") < lower_bound_braking)
            .then(lower_bound_braking)
            .otherwise(pl.col("longitudinal_acceleration"))
            .alias("longitudinal_acceleration"),
        )
        df = df.with_columns(
            pl.when(pl.col("longitudinal_acceleration") > upper_bound_acceleration)
            .then(-upper_bound_acceleration)
            .otherwise(pl.col("longitudinal_acceleration"))
            .alias("longitudinal_acceleration"),
            pl.when(pl.col("lateral_acceleration") < lower_bound_lateral)
            .then(lower_bound_lateral)
            .otherwise(pl.col("lateral_acceleration"))
            .alias("lateral_acceleration"),
        )
    return df


def enrich_individual_telemetry_parquet_files(context, df) -> None:
    """For each small telemetry file, add accelerations and digital filters, running operations in parallel (because this is an IO bound operation). Write the dfs to a separate directory with parquet files."""
    df = parse_telemetry_timestamps(context, df)
    df = telemetry_coordinate_calculations(context, df)
    df = enrich_fastf1_telemetry(context, df)
    df = handle_outliers(context, df)
    df = apply_digital_filter(context, df)
    df = handle_outliers(
        context, df, handling="winsorize"
    )  # this is to handle any outliers that may have been introduced by the filter
    return df
