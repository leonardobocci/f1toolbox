import os, sys
sys.path.append(os.path.abspath('./'))

import requests
import fastf1
import flatdict

from dagster import asset, MetadataValue
from partitions import fast_f1_season_partitions, fast_f1_multi_partitions
from utils.fastf1_extractor import extract_race_events
from fastf1.core import DataNotLoadedError

from assets.constants import SEASONS as years
from assets.constants import SEGMENTS as segments

@asset(
    group_name='raw_fastf1_files',
    partitions_def=fast_f1_season_partitions
)
def landing_fastf1_events(context):
    year = context.partition_key
    saved_events = extract_race_events(context, int(year))
    context.add_output_metadata({'Number of Saved Events':MetadataValue.int(len(saved_events['saved']))})
    context.add_output_metadata({'Total number of Events':MetadataValue.int(saved_events['total'])})
    return