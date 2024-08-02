import glob
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import polars as pl
import scipy.signal as ss

from src.dagster.assets import constants
from src.dagster.utils import iomanager


def parse_json_signals(context, signal_directory: str) -> pl.DataFrame:
    """Given an asset subdirectory, load all json files for all years and return a polars dataframe."""
    glob_path = glob.glob(f"data/landing/fastf1/*/{signal_directory}/*.json")
    dfs = [pl.read_json(file) for file in glob_path]
    df = pl.concat(dfs)
    return df


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
        ).collect(streaming=True)
    else:
        df = pl.scan_parquet(f"data/landing/fastf1/*/{signal_directory}/*.parquet")
    return df


def parse_session_timestamps(context, df: pl.LazyFrame) -> pl.LazyFrame:
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
    return df


def parse_results_lap_times(context, df: pl.LazyFrame) -> pl.LazyFrame:
    df = df.with_columns(
        lap_time_seconds=pl.col("Time") / 1e9,  # from nanoseconds to seconds
        q1_lap_time_seconds=pl.col("Q1") / 1e9,  # from nanoseconds to seconds
        q2_lap_time_seconds=pl.col("Q2") / 1e9,  # from nanoseconds to seconds
        q3_lap_time_seconds=pl.col("Q3") / 1e9,  # from nanoseconds to seconds
    )
    return df.select(pl.exclude("Time", "Q1", "Q2", "Q3"))


def parse_lap_timestamps(context, df: pl.LazyFrame) -> pl.LazyFrame:
    return df.with_columns(
        end_time=pl.col("Time").shift(-1, fill_value=df.select(pl.last("Time")))
    )  # last value would be null otherwise


def parse_weather_timestamps(
    context, df: pl.LazyFrame, sessions_df: pl.LazyFrame
) -> pl.LazyFrame:
    """Given a weather dataframe, parse the timestamp and return a new column with the time in seconds from the start of the session."""
    selection = df.columns
    df = df.join(sessions_df, left_on="session_id", right_on="id")
    df = df.with_columns(
        seconds_from_session_start=pl.col("Time") / 1e9  # from nanoseconds to seconds
    )
    df = df.with_columns(
        timestamp=pl.col("seconds_from_session_start") + pl.col("local_start_datetime"),
    )
    df = df.with_columns(
        end_timestamp=pl.col("seconds_from_session_start").shift(
            -1, fill_value=pl.col("seconds_from_session_start").last()
        )
    )
    df = df.select(*selection, "timestamp", "end_timestamp")
    df = df.select(pl.exclude("Time"))
    return df


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
                (pl.col("x_prev_2") - pl.col("x_prev_1")).alias("ux"),
                (pl.col("y_prev_2") - pl.col("y_prev_1")).alias("uy"),
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
        results = temp_df.select([*df.columns, "center_x", "center_y"])
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
        selection = df.columns
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
        # divide by 9.80665 to get g's
        context.log.debug("Added lateral acceleration.")
        return results

    def get_car_longitudinal_load(context, df: pl.LazyFrame) -> pl.LazyFrame:
        selection = df.columns
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
        # divide by 9.80665 to get g's
        context.log.debug("Added longitudinal acceleration.")
        return results

    selection = df.columns
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


def enrich_individual_telemetry_parquet_files(context) -> None:
    """For each small telemetry file, add accelerations and digital filters, running operations in parallel (because this is an IO bound operation). Write the dfs to a separate directory with parquet files."""

    def extract_from_path(path, target="filenum"):
        if target == "filenum":
            match = re.search(r"telemetry/(\d+).parquet", path)
        elif target == "year":
            match = re.search(r"fastf1/(\d+)/telemetry", path)
        return match.group(1)

    def process_file(path):
        file_number = extract_from_path(str(path), target="filenum")
        year = extract_from_path(str(path), target="year")
        df = pl.scan_parquet(path)
        df = telemetry_coordinate_calculations(context, df)
        df = enrich_fastf1_telemetry(context, df)
        df = handle_outliers(context, df)
        df = apply_digital_filter(context, df)
        df = handle_outliers(
            context, df, handling="winsorize"
        )  # this is to handle any outliers that may have been introduced by the filter
        iomanager.polars_to_parquet(
            filedir=f"{constants.landing_FASTF1_PATH}/{year}/rich_telemetry",
            filename=file_number,
            data=df,
            context=context,
        )

    paths_list = [
        x for x in Path(constants.landing_FASTF1_PATH).rglob("*/telemetry/*.parquet")
    ]

    with ThreadPoolExecutor(
        max_workers=2
    ) as executor:  # TODO: SMART SETTING FOR MAX WORKERS
        futures = [executor.submit(process_file, path) for path in paths_list]
        for future in as_completed(futures):
            future.result()  # Wait for each future to complete. Raise exceptions.

    return None


def create_telemetry_scores(context, df: pl.LazyFrame) -> pl.LazyFrame:
    """
    TODO:
    calculate acceleration scores (per session, taking fastest lap only, and normalizing tyre difference, to eliminate changing conditions)

    quali: fastest lap only to get scores - ensure same tyre compound
    race+sprint: all laps + normalize tyre compound based on degradation curve for tyre age and tyre compound mean difference.
    """
    pass
