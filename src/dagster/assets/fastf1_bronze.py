from pyarrow.parquet import read_metadata as parquet_metadata

from dagster import MetadataValue, asset
from src.dagster.assets import constants
from src.dagster.utils.fastf1_parser import (
    enrich_individual_telemetry_parquet_files,
    parse_json_signals,
    parse_lap_timestamps,
    parse_parquet_signals,
    parse_results_lap_times,
    parse_session_timestamps,
    parse_weather_timestamps,
)
from src.dagster.utils.iomanager import parquet_to_polars, polars_to_parquet


@asset(
    group_name="bronze_fastf1_files",
    deps=["landing_fastf1_assets"],
    compute_kind="polars",
)
def bronze_fastf1_events(context):
    """Parse landing zone fastf1 event details to parquet file"""
    df = parse_json_signals(context, "events")
    polars_to_parquet(
        filedir=constants.BRONZE_FASTF1_PATH,
        filename="events",
        data=df,
        context=context,
    )
    meta = parquet_metadata(f"{constants.BRONZE_FASTF1_PATH}/events.parquet").to_dict()
    context.add_output_metadata({"Rows": MetadataValue.int(meta["num_rows"])})
    context.add_output_metadata({"Columns": MetadataValue.int(meta["num_columns"])})
    return


@asset(
    group_name="bronze_fastf1_files",
    deps=["landing_fastf1_assets"],
    compute_kind="polars",
)
def bronze_fastf1_laps(context):
    """Parse landing zone fastf1 lap details to parquet file"""
    df = parse_parquet_signals(context, "laps")
    df = parse_lap_timestamps(context, df)
    polars_to_parquet(
        filedir=constants.BRONZE_FASTF1_PATH, filename="laps", data=df, context=context
    )
    meta = parquet_metadata(f"{constants.BRONZE_FASTF1_PATH}/laps.parquet").to_dict()
    context.add_output_metadata({"Rows": MetadataValue.int(meta["num_rows"])})
    context.add_output_metadata({"Columns": MetadataValue.int(meta["num_columns"])})
    return


@asset(
    group_name="bronze_fastf1_files",
    deps=["landing_fastf1_assets"],
    compute_kind="polars",
)
def bronze_fastf1_results(context):
    """Parse landing zone fastf1 results details to parquet file"""
    df = parse_parquet_signals(context, "results")
    df = parse_results_lap_times(context, df)
    polars_to_parquet(
        filedir=constants.BRONZE_FASTF1_PATH,
        filename="results",
        data=df,
        context=context,
    )
    meta = parquet_metadata(f"{constants.BRONZE_FASTF1_PATH}/results.parquet").to_dict()
    context.add_output_metadata({"Rows": MetadataValue.int(meta["num_rows"])})
    context.add_output_metadata({"Columns": MetadataValue.int(meta["num_columns"])})
    return


@asset(
    group_name="bronze_fastf1_files",
    deps=["landing_fastf1_assets"],
    compute_kind="polars",
)
def bronze_fastf1_sessions(context):
    """Parse landing zone fastf1 sessions details to parquet file"""
    df = parse_json_signals(context, "sessions")
    df = parse_session_timestamps(context, df)
    polars_to_parquet(
        filedir=constants.BRONZE_FASTF1_PATH,
        filename="sessions",
        data=df,
        context=context,
    )
    meta = parquet_metadata(
        f"{constants.BRONZE_FASTF1_PATH}/sessions.parquet"
    ).to_dict()
    context.add_output_metadata({"Rows": MetadataValue.int(meta["num_rows"])})
    context.add_output_metadata({"Columns": MetadataValue.int(meta["num_columns"])})
    return


@asset(
    group_name="bronze_fastf1_files",
    deps=["landing_fastf1_assets"],
    compute_kind="polars",
)
def bronze_fastf1_telemetry(context):
    """Parse landing zone fastf1 telemetry details to parquet file"""
    enrich_individual_telemetry_parquet_files(
        context
    )  # saves to a rich_telemetry directory
    df = parse_parquet_signals(context, "rich_telemetry")
    polars_to_parquet(
        filedir=constants.BRONZE_FASTF1_PATH,
        filename="telemetry",
        data=df,
        context=context,
    )
    meta = parquet_metadata(
        f"{constants.BRONZE_FASTF1_PATH}/telemetry.parquet"
    ).to_dict()
    context.add_output_metadata({"Rows": MetadataValue.int(meta["num_rows"])})
    context.add_output_metadata({"Columns": MetadataValue.int(meta["num_columns"])})
    return


@asset(
    group_name="bronze_fastf1_files",
    deps=["landing_fastf1_assets"],
    compute_kind="polars",
)
def bronze_fastf1_circuit_corners(context):
    """Parse landing zone fastf1 weathers details to parquet file"""
    df = parse_parquet_signals(context, "circuit_corners")
    polars_to_parquet(
        filedir=constants.BRONZE_FASTF1_PATH,
        filename="circuit_corners",
        data=df,
        context=context,
    )
    meta = parquet_metadata(
        f"{constants.BRONZE_FASTF1_PATH}/circuit_corners.parquet"
    ).to_dict()
    context.add_output_metadata({"Rows": MetadataValue.int(meta["num_rows"])})
    context.add_output_metadata({"Columns": MetadataValue.int(meta["num_columns"])})
    return


@asset(
    group_name="bronze_fastf1_files",
    deps=["landing_fastf1_assets", "bronze_fastf1_sessions"],
    compute_kind="polars",
)
def bronze_fastf1_weathers(context):
    """Parse landing zone fastf1 weathers details to parquet file"""
    sessions = parquet_to_polars(
        f"{constants.BRONZE_FASTF1_PATH}/sessions.parquet", context
    )
    df = parse_parquet_signals(context, "weathers")
    df = parse_weather_timestamps(context, df, sessions)
    polars_to_parquet(
        filedir=constants.BRONZE_FASTF1_PATH,
        filename="weathers",
        data=df,
        context=context,
    )
    meta = parquet_metadata(
        f"{constants.BRONZE_FASTF1_PATH}/weathers.parquet"
    ).to_dict()
    context.add_output_metadata({"Rows": MetadataValue.int(meta["num_rows"])})
    context.add_output_metadata({"Columns": MetadataValue.int(meta["num_columns"])})
    return


@asset(
    group_name="bronze_fastf1_files",
    deps=["landing_fastf1_assets"],
    compute_kind="polars",
)
def bronze_fastf1_tyres(context):
    """Parse landing zone fastf1 tyre compound details to parquet file"""
    df = parse_parquet_signals(context, "tyre_compounds")
    polars_to_parquet(
        filedir=constants.BRONZE_FASTF1_PATH,
        filename="tyre_compounds",
        data=df,
        context=context,
    )
    meta = parquet_metadata(
        f"{constants.BRONZE_FASTF1_PATH}/tyre_compounds.parquet"
    ).to_dict()
    context.add_output_metadata({"Rows": MetadataValue.int(meta["num_rows"])})
    context.add_output_metadata({"Columns": MetadataValue.int(meta["num_columns"])})
    return
