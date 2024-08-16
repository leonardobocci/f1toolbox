from dagster import MetadataValue, RetryPolicy, asset

from src.dagster.partitions import fast_f1_season_partitions
from src.dagster.utils.fastf1_extractor import extract_fastf1


@asset(
    group_name="landing_fastf1_files",
    partitions_def=fast_f1_season_partitions,
    compute_kind="python",
    retry_policy=RetryPolicy(max_retries=1, delay=5),
)
def landing_fastf1_assets(context):
    """Extract all data streams from fasft1 and save to landing zone."""
    year = context.partition_key
    meta = extract_fastf1(context, int(year))
    context.add_output_metadata(
        {"Number of Saved Events": MetadataValue.int(len(meta["saved_events"]))}
    )
    context.add_output_metadata(
        {"Total number of Events": MetadataValue.int(meta["total_events"])}
    )
    return
