from dagster import AssetSelection, define_asset_job
from src.dagster.partitions import fantasy_partitions, fast_f1_season_partitions

fastf1_assets = AssetSelection.groups("landing_fastf1_files", "bronze_fastf1_files")
fantasy_assets = AssetSelection.groups("landing_fantasy_files", "bronze_fantasy_files")
dbt_assets = AssetSelection.groups(
    "bronze_fantasy_views", "bronze_fastf1_views", "silver_views", "gold_"
)

refresh_fantasy = define_asset_job(
    name="refresh_fantasy",
    partitions_def=fantasy_partitions,
    selection=fantasy_assets,
)

refresh_fastf1 = define_asset_job(
    name="refresh_fastf1",
    partitions_def=fast_f1_season_partitions,
    selection=fastf1_assets,
)

dbt_refresh = define_asset_job(
    name="refresh_bronze_dbt",
    selection=dbt_assets,
)
