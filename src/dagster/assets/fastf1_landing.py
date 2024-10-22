from dagster import AssetOut, multi_asset
from src.dagster.partitions import fast_f1_season_partitions
from src.dagster.utils.fastf1_extractor import extract_fastf1

io_shortcuts = {
    "landing_json": AssetOut(io_manager_key="gcs_json_fastf1_landing_io_manager"),
    "landing_parquet": AssetOut(io_manager_key="gcs_parquet_fastf1_landing_io_manager"),
}


@multi_asset(
    group_name="landing_fastf1_files",
    partitions_def=fast_f1_season_partitions,
    compute_kind="python",
    outs={
        "tyre_compounds": io_shortcuts["landing_parquet"],
        "events": io_shortcuts["landing_json"],
        "circuit_corners": io_shortcuts["landing_parquet"],
        "sessions": io_shortcuts["landing_json"],
        "session_results": io_shortcuts["landing_parquet"],
        "weather": io_shortcuts["landing_parquet"],
        "laps": io_shortcuts["landing_parquet"],
        "telemetry": io_shortcuts["landing_parquet"],
    },
)
def landing_fastf1_assets(context):
    """Extract all data streams from fasft1 and save to landing zone."""
    year = context.partition_key
    (
        meta,
        tyre_compounds,
        event_info,
        circuit_corners,
        session,
        session_results,
        weather,
        laps,
        telemetry,
    ) = extract_fastf1(context, int(year))
    context.log.info(f"Recorded the following metadata: {meta}.")
    return (
        tyre_compounds,
        event_info,
        circuit_corners,
        session,
        session_results,
        weather,
        laps,
        telemetry,
    )
