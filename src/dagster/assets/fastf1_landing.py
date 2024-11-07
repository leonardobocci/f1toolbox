import polars as pl

from dagster import AssetIn, AssetOut, asset, multi_asset
from src.dagster.partitions import fast_f1_partitions, fast_f1_season_partitions
from src.dagster.utils.fastf1_extractor import (
    extract_event,
    extract_fastf1,
    extract_tyre_compounds,
    validate_event_num,
)
from src.dagster.utils.fastf1_parser import enrich_individual_telemetry_parquet_files

io_shortcuts = {
    "landing_json": AssetOut(io_manager_key="gcs_json_fastf1_landing_io_manager"),
    "landing_parquet": AssetOut(io_manager_key="gcs_parquet_fastf1_landing_io_manager"),
}


@asset(
    group_name="landing_fastf1_season_files",
    partitions_def=fast_f1_season_partitions,
    compute_kind="python",
    op_tags={"dagster/concurrency_key": "fastf1"},
    io_manager_key="gcs_parquet_fastf1_landing_io_manager",
)
def landing_fastf1_tyre_compounds(context):
    year = int(context.partition_key)
    return extract_tyre_compounds(year)


@multi_asset(
    group_name="landing_fastf1_files",
    partitions_def=fast_f1_partitions,
    compute_kind="python",
    op_tags={"dagster/concurrency_key": "fastf1"},
    outs={
        "landing_fastf1_events": io_shortcuts["landing_json"],
        "landing_fastf1_circuit_corners": io_shortcuts["landing_parquet"],
    },
)
def landing_fastf1_event_assets(context):
    year, event_num = validate_event_num(context)
    if year and event_num:
        return extract_event(context, year, event_num)
    else:
        return (None, None)


@multi_asset(
    group_name="landing_fastf1_files",
    partitions_def=fast_f1_partitions,
    compute_kind="python",
    op_tags={"dagster/concurrency_key": "fastf1"},
    outs={
        "landing_fastf1_sessions": io_shortcuts["landing_json"],
        "landing_fastf1_session_results": io_shortcuts["landing_parquet"],
        "landing_fastf1_weather": io_shortcuts["landing_parquet"],
        "landing_fastf1_laps": io_shortcuts["landing_parquet"],
        "landing_fastf1_telemetry": io_shortcuts["landing_parquet"],
    },
)
def landing_fastf1_session_assets(context):
    """Extract all data streams from fasft1 and save to landing zone."""
    # validate if the event exists for the year
    year, event_num = validate_event_num(context)
    if not event_num:
        return (None, None, None, None, None)
    (
        meta,
        landing_fastf1_session,
        landing_fastf1_session_results,
        landing_fastf1_weather,
        landing_fastf1_laps,
        landing_fastf1_telemetry,
    ) = extract_fastf1(context, year, event_num)
    context.log.info(f"Recorded the following metadata: {meta}.")
    return (
        landing_fastf1_session,
        landing_fastf1_session_results,
        landing_fastf1_weather,
        landing_fastf1_laps,
        landing_fastf1_telemetry,
    )


@asset(
    group_name="landing_fastf1_files",
    ins={"landing_fastf1_telemetry": AssetIn()},
    partitions_def=fast_f1_partitions,
    compute_kind="python",
    io_manager_key="gcs_parquet_fastf1_landing_io_manager",
)
def landing_fastf1_rich_telemetry(context, landing_fastf1_telemetry):
    year, event_num = validate_event_num(context)
    df = pl.scan_parquet(landing_fastf1_telemetry)
    if not event_num:
        return None
    return enrich_individual_telemetry_parquet_files(
        context, df
    )  # saves to a rich_telemetry directory
