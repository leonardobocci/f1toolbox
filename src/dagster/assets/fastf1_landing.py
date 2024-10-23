from dagster import AssetOut, multi_asset
from src.dagster.partitions import fast_f1_season_partitions
from src.dagster.utils.fastf1_extractor import extract_fastf1
from src.dagster.utils.fastf1_parser import enrich_individual_telemetry_parquet_files

io_shortcuts = {
    "landing_json": AssetOut(io_manager_key="gcs_json_fastf1_landing_io_manager"),
    "landing_parquet": AssetOut(io_manager_key="gcs_parquet_fastf1_landing_io_manager"),
}


@multi_asset(
    group_name="landing_fastf1_files",
    partitions_def=fast_f1_season_partitions,
    compute_kind="python",
    outs={
        "landing_fastf1_tyre_compounds": io_shortcuts["landing_parquet"],
        "landing_fastf1_events": io_shortcuts["landing_json"],
        "landing_fastf1_circuit_corners": io_shortcuts["landing_parquet"],
        "landing_fastf1_sessions": io_shortcuts["landing_json"],
        "landing_fastf1_session_results": io_shortcuts["landing_parquet"],
        "landing_fastf1_weather": io_shortcuts["landing_parquet"],
        "landing_fastf1_laps": io_shortcuts["landing_parquet"],
        "landing_fastf1_telemetry": io_shortcuts["landing_parquet"],
    },
)
def landing_fastf1_assets(context):
    """Extract all data streams from fasft1 and save to landing zone."""
    year = context.partition_key
    (
        meta,
        landing_fastf1_tyre_compounds,
        landing_fastf1_event_info,
        landing_fastf1_circuit_corners,
        landing_fastf1_session,
        landing_fastf1_session_results,
        landing_fastf1_weather,
        landing_fastf1_laps,
        landing_fastf1_telemetry,
    ) = extract_fastf1(context, int(year))
    context.log.info(f"Recorded the following metadata: {meta}.")
    context.log.info("Now working on telemetry enrichment...")
    landing_fastf1_telemetry = enrich_individual_telemetry_parquet_files(
        context, landing_fastf1_telemetry
    )  # saves to a rich_telemetry directory
    return (
        landing_fastf1_tyre_compounds,
        landing_fastf1_event_info,
        landing_fastf1_circuit_corners,
        landing_fastf1_session,
        landing_fastf1_session_results,
        landing_fastf1_weather,
        landing_fastf1_laps,
        landing_fastf1_telemetry,
    )
