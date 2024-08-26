from dagster import AssetSelection, define_asset_job
from src.dagster.partitions import fantasy_partitions, fast_f1_season_partitions

landing_fantasy_assets = AssetSelection.groups("landing_fantasy_files")
landing_fastf1_assets = AssetSelection.groups("landing_fastf1_files")
bronze_fantasy_files_assets = AssetSelection.groups("bronze_fantasy_files")
bronze_fastf1_files_assets = AssetSelection.groups("bronze_fastf1_files")
bronze_fantasy_views_assets = AssetSelection.groups("bronze_fantasy_views")
bronze_fastf1_views_assets = AssetSelection.groups("bronze_fastf1_views")
silver_views_assets = AssetSelection.groups("silver_views")
gold_incremental_assets = AssetSelection.groups("gold_incremental_views")
marts_incremental_assets = AssetSelection.groups("marts_incremental_views")

landing_fastf1_full_job = define_asset_job(
    name="fastf1_landing_full",
    partitions_def=fast_f1_season_partitions,
    selection=landing_fastf1_assets,
    config={
        "execution": {
            "config": {
                "multiprocess": {
                    "max_concurrent": 1,  # prevents fastf1 errors from concurrent runs
                },
            }
        }
    },
)

landing_fantasy_full_job = define_asset_job(
    name="fantasy_landing_full",
    partitions_def=fantasy_partitions,
    selection=landing_fantasy_assets,
)
