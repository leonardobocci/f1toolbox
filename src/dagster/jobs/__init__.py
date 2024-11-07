from dagster import AssetSelection, define_asset_job
from src.dagster.partitions import (
    fantasy_partitions,
    fast_f1_partitions,
    fast_f1_season_partitions,
)

fastf1_season_assets = AssetSelection.groups("landing_fastf1_season_files")
fastf1_landing_assets = AssetSelection.groups("landing_fastf1_files")
fantasy_landing_assets = AssetSelection.groups("landing_fantasy_files")

fastf1_bronze_assets = AssetSelection.groups("bronze_fastf1_files")
fantasy_bronze_assets = AssetSelection.groups("bronze_fantasy_files")

dbt_assets = AssetSelection.groups(
    "bronze_fantasy_views", "bronze_fastf1_views", "silver_views", "gold_"
)

refresh_fantasy_landing = define_asset_job(
    name="refresh_fantasy_landing",
    partitions_def=fantasy_partitions,
    selection=fantasy_landing_assets,
)

refresh_fantasy_bronze = define_asset_job(
    name="refresh_fantasy_bronze",
    selection=fantasy_bronze_assets,
)

refresh_season_fastf1_landing = define_asset_job(
    name="refresh_season_fastf1_landing",
    partitions_def=fast_f1_season_partitions,
    selection=fastf1_season_assets,
)

refresh_fastf1_landing = define_asset_job(
    name="refresh_fastf1_landing",
    partitions_def=fast_f1_partitions,
    selection=fastf1_landing_assets,
)

refresh_fastf1_bronze = define_asset_job(
    name="refresh_fastf1_bronze",
    selection=fastf1_bronze_assets,
)

dbt_refresh = define_asset_job(
    name="refresh_bronze_dbt",
    selection=dbt_assets,
)
