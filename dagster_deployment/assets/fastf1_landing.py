import os, sys
sys.path.append(os.path.abspath('./'))

import requests
import fastf1
import flatdict

from dagster import asset, MetadataValue
from partitions import fast_f1_season_partitions
from utils.fastf1_extractor import extract_fastf1

@asset(
    group_name='raw_fastf1_files',
    partitions_def=fast_f1_season_partitions
)
def landing_fastf1_events(context):
    year = context.partition_key
    meta = extract_fastf1(context, int(year))
    context.add_output_metadata({'Number of Saved Events':MetadataValue.int(len(meta['saved_events']))})
    context.add_output_metadata({'Total number of Events':MetadataValue.int(meta['total_events'])})
    
    return