from dagster import AssetSelection, define_asset_job
from partitions import fantasy_partitions, fast_f1_season_partitions

fantasy_assets = AssetSelection.groups("raw_fantasy_files")
fastf1_assets = AssetSelection.groups("raw_fastf1_files")

landing_fastf1_full_job = define_asset_job(
    name="fastf1_landing_full",
    partitions_def=fast_f1_season_partitions,
    selection=fastf1_assets,
)

landing_fantasy_full_job = define_asset_job(
    name="fantasy_landing_full",
    partitions_def=fantasy_partitions,
    selection=fantasy_assets,
)
