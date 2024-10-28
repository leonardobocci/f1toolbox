import polars as pl

from dagster import AssetIn, asset
from src.dagster.utils.fastf1_parser import (
    parse_lap_timestamps,
    parse_results_lap_times,
    parse_session_timestamps,
    parse_weather_timestamps,
)


@asset(
    group_name="bronze_fastf1_files",
    compute_kind="polars",
    ins={"landing_fastf1_events": AssetIn()},
    io_manager_key="gcs_parquet_fastf1_bronze_io_manager",
)
def bronze_fastf1_events(context, landing_fastf1_events):
    """Parse landing zone fastf1 event details to parquet file"""
    dfs = [pl.LazyFrame(data) for data in landing_fastf1_events]
    df = pl.concat(dfs)
    return df


@asset(
    group_name="bronze_fastf1_files",
    ins={"landing_fastf1_laps": AssetIn()},
    compute_kind="polars",
    io_manager_key="gcs_parquet_fastf1_bronze_io_manager",
)
def bronze_fastf1_laps(context, landing_fastf1_laps):
    """Parse landing zone fastf1 lap details to parquet file"""
    df = pl.concat(landing_fastf1_laps)
    df = parse_lap_timestamps(context, df)
    return df


@asset(
    group_name="bronze_fastf1_files",
    ins={"landing_fastf1_session_results": AssetIn()},
    compute_kind="polars",
    io_manager_key="gcs_parquet_fastf1_bronze_io_manager",
)
def bronze_fastf1_session_results(context, landing_fastf1_session_results):
    """Parse landing zone fastf1 results details to parquet file"""
    df = pl.concat(landing_fastf1_session_results)
    df = parse_results_lap_times(context, df)
    return df


@asset(
    group_name="bronze_fastf1_files",
    compute_kind="polars",
    ins={"landing_fastf1_sessions": AssetIn()},
    io_manager_key="gcs_parquet_fastf1_bronze_io_manager",
)
def bronze_fastf1_sessions(context, landing_fastf1_sessions):
    """Parse landing zone fastf1 sessions details to parquet file"""
    dfs = [pl.LazyFrame(data) for data in landing_fastf1_sessions]
    df = pl.concat(dfs)
    df = parse_session_timestamps(context, df)
    return df


@asset(
    group_name="bronze_fastf1_files",
    compute_kind="polars",
    # not produced by multiasset so no need for ins
    io_manager_key="gcs_parquet_fastf1_bronze_io_manager",
)
def bronze_fastf1_telemetry(context, landing_fastf1_rich_telemetry):
    """Parse landing zone fastf1 results details to parquet file"""
    df = pl.concat(landing_fastf1_rich_telemetry)
    return df


@asset(
    group_name="bronze_fastf1_files",
    ins={"landing_fastf1_circuit_corners": AssetIn()},
    compute_kind="polars",
    io_manager_key="gcs_parquet_fastf1_bronze_io_manager",
)
def bronze_fastf1_circuit_corners(context, landing_fastf1_circuit_corners):
    """Parse landing zone fastf1 weathers details to parquet file"""
    df = pl.concat(landing_fastf1_circuit_corners)
    return df


@asset(
    group_name="bronze_fastf1_files",
    ins={"landing_fastf1_weather": AssetIn()},
    compute_kind="polars",
    io_manager_key="gcs_parquet_fastf1_bronze_io_manager",
)
def bronze_fastf1_weathers(context, landing_fastf1_weather, bronze_fastf1_sessions):
    """Parse landing zone fastf1 weathers details to parquet file"""
    df = pl.concat(landing_fastf1_weather)
    df = parse_weather_timestamps(context, df, bronze_fastf1_sessions)
    return df


@asset(
    group_name="bronze_fastf1_files",
    # not produced by multiasset so no need for ins
    compute_kind="polars",
    io_manager_key="gcs_parquet_fastf1_bronze_io_manager",
)
def bronze_fastf1_tyres(context, landing_fastf1_tyre_compounds):
    """Parse landing zone fastf1 tyre compound details to parquet file"""
    df = pl.concat(landing_fastf1_tyre_compounds)
    return df
