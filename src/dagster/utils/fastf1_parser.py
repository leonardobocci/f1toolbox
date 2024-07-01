import glob
from pathlib import Path

import polars as pl


def parse_json_signals(context, signal_directory: str) -> pl.DataFrame:
    """Given an asset subdirectory, load all json files for all years and return a polars dataframe."""
    glob_path = glob.glob(f"data/landing/fastf1/*/{signal_directory}/*.json")
    dfs = [pl.read_json(file) for file in glob_path]
    df = pl.concat(dfs)
    return df


def parse_parquet_signals(context, signal_directory: str) -> pl.LazyFrame:
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
                (pl.col("Speed") / 3.6).alias("delta_ms_speed"),
                (pl.col("delta_time") / 1e6).alias("delta_s_time"),
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
    return results


def create_telemetry_scores(context, df: pl.LazyFrame) -> pl.LazyFrame:
    '''
    TODO: 
    eliminate outliers (winsorize)
    calculate acceleration scores (per session, to eliminate changing conditions)
    '''
    pass
