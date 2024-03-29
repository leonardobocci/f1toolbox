import os, sys
sys.path.append(os.path.abspath('./'))

from dagster import asset, MetadataValue
from assets import constants
from utils.iomanager import polars_to_parquet
from utils.fastf1_parser import parse_parquet_signals, parse_json_signals
from pyarrow.parquet import read_metadata as parquet_metadata

@asset(
    group_name='bronze_fastf1_files',
    deps=["landing_fastf1_assets"]
)
def bronze_fastf1_events(context):
    '''Parse landing zone fastf1 event details to parquet file'''
    df = parse_json_signals(context, 'events')
    polars_to_parquet(filedir=constants.BRONZE_FASTF1_PATH, filename='events', data=df)   
    meta = parquet_metadata(f'{constants.BRONZE_FASTF1_PATH}/events.parquet').to_dict()
    context.add_output_metadata({'Rows':MetadataValue.int(meta['num_rows'])})
    context.add_output_metadata({'Columns':MetadataValue.int(meta['num_columns'])})
    return

@asset(
    group_name='bronze_fastf1_files',
    deps=["landing_fastf1_assets"]
)
def bronze_fastf1_laps(context):
    '''Parse landing zone fastf1 lap details to parquet file'''
    df = parse_parquet_signals(context, 'laps')
    polars_to_parquet(filedir=constants.BRONZE_FASTF1_PATH, filename='laps', data=df)   
    meta = parquet_metadata(f'{constants.BRONZE_FASTF1_PATH}/laps.parquet').to_dict()
    context.add_output_metadata({'Rows':MetadataValue.int(meta['num_rows'])})
    context.add_output_metadata({'Columns':MetadataValue.int(meta['num_columns'])})
    return

@asset(
    group_name='bronze_fastf1_files',
    deps=["landing_fastf1_assets"]
)
def bronze_fastf1_results(context):
    '''Parse landing zone fastf1 results details to parquet file'''
    df = parse_parquet_signals(context, 'results')
    polars_to_parquet(filedir=constants.BRONZE_FASTF1_PATH, filename='results', data=df)   
    meta = parquet_metadata(f'{constants.BRONZE_FASTF1_PATH}/results.parquet').to_dict()
    context.add_output_metadata({'Rows':MetadataValue.int(meta['num_rows'])})
    context.add_output_metadata({'Columns':MetadataValue.int(meta['num_columns'])})
    return

@asset(
    group_name='bronze_fastf1_files',
    deps=["landing_fastf1_assets"]
)
def bronze_fastf1_sessions(context):
    '''Parse landing zone fastf1 sessions details to parquet file'''
    df = parse_json_signals(context, 'sessions')
    polars_to_parquet(filedir=constants.BRONZE_FASTF1_PATH, filename='sessions', data=df)   
    meta = parquet_metadata(f'{constants.BRONZE_FASTF1_PATH}/sessions.parquet').to_dict()
    context.add_output_metadata({'Rows':MetadataValue.int(meta['num_rows'])})
    context.add_output_metadata({'Columns':MetadataValue.int(meta['num_columns'])})
    return

@asset(
    group_name='bronze_fastf1_files',
    deps=["landing_fastf1_assets"]
)
def bronze_fastf1_telemetry(context):
    '''Parse landing zone fastf1 telemetry details to parquet file'''
    df = parse_parquet_signals(context, 'telemetry')
    polars_to_parquet(filedir=constants.BRONZE_FASTF1_PATH, filename='telemetry', data=df)   
    meta = parquet_metadata(f'{constants.BRONZE_FASTF1_PATH}/telemetry.parquet').to_dict()
    context.add_output_metadata({'Rows':MetadataValue.int(meta['num_rows'])})
    context.add_output_metadata({'Columns':MetadataValue.int(meta['num_columns'])})
    return

@asset(
    group_name='bronze_fastf1_files',
    deps=["landing_fastf1_assets"]
)
def bronze_fastf1_weathers(context):
    '''Parse landing zone fastf1 weathers details to parquet file'''
    df = parse_parquet_signals(context, 'weathers')
    polars_to_parquet(filedir=constants.BRONZE_FASTF1_PATH, filename='weathers', data=df)   
    meta = parquet_metadata(f'{constants.BRONZE_FASTF1_PATH}/weathers.parquet').to_dict()
    context.add_output_metadata({'Rows':MetadataValue.int(meta['num_rows'])})
    context.add_output_metadata({'Columns':MetadataValue.int(meta['num_columns'])})
    return