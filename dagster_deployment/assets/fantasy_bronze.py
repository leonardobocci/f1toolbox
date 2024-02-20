import os, sys
sys.path.append(os.path.abspath('./'))

import json
import logging
import polars as pl
from dagster import asset, MetadataValue
from partitions import fantasy_partitions
from assets import constants
from utils.iomanager import polars_to_parquet
from pyarrow.parquet import read_metadata as parquet_metadata

def format_raw_df(df:pl.DataFrame, lookup_df:pl.DataFrame) -> pl.DataFrame:
    '''Format the constructors and drivers dataframes'''
    df = df.with_columns((last_modified_expr).alias('last_updated'))
    df1 = df.join(lookup_df, left_on='abbreviation', right_on='id', how='outer')
    df1 = df1.select(
        *set(lookup_df.columns) - set(['id', 'color']),
        pl.coalesce('id', 'abbreviation').alias('id'),
        pl.coalesce('color', 'color_right').alias('color'),
        pl.col('last_updated').fill_null(strategy="min"))
    return df1

def read_landing_fantasy_assets():
    with open(f'{constants.RAW_FANTASY_PATH}/current_fantasy_assets.json', 'r') as f:
        file = json.load(f)
    last_modified = os.path.getmtime(f'{constants.RAW_FANTASY_PATH}/current_fantasy_assets.json')
    last_modified_expr = pl.from_epoch(pl.lit(last_modified))
    return file, last_modified_expr

@asset(
    group_name='bronze_fantasy_files',
    deps=["landing_fantasy_current_assets"]
)
def bronze_fantasy_current_constructors(context):
    created=False
    constructor_lookup = pl.scan_csv('utils/constructor_mapping.csv')
    unique_constructor_list = pl.scan_parquet(f'{constants.BRONZE_FANTASY_PATH}/constructor_fantasy_attributes.parquet').select('id', 'color').unique()
    constructor_lookup = unique_constructor_list.join(constructor_lookup, on='id', how='outer')
    constructor_lookup = constructor_lookup.select(pl.coalesce('id', 'id_right').alias('id'), 'name', 'active', 'color')
    file, last_modified_expr = read_landing_fantasy_assets()
    constructors = pl.LazyFrame(file['constructors'])
    constructors = format_raw_df(constructors, constructor_lookup)
    polars_to_parquet(filedir=constants.BRONZE_FANTASY_PATH, filename='constructors', data=constructors)
    constructors_meta = parquet_metadata(f'{constants.BRONZE_FANTASY_PATH}/constructors.parquet').to_dict()
    context.add_output_metadata({'Rows':MetadataValue.int(meta['num_rows'])})
    context.add_output_metadata({'Columns':MetadataValue.int(meta['num_columns'])})


@asset(
    group_name='bronze_fantasy_files',
    deps=["landing_fantasy_current_assets"]
)
def bronze_fantasy_current_drivers(context):
    created=False
    drivers_lookup = pl.scan_csv('utils/driver_mapping.csv')
    unique_driver_list = pl.scan_parquet(f'{constants.BRONZE_FANTASY_PATH}/driver_fantasy_attributes.parquet').select('id', 'color').unique()
    drivers_lookup = unique_driver_list.join(drivers_lookup, on='id', how='outer')
    drivers_lookup = drivers_lookup.select(pl.coalesce('id', 'id_right').alias('id'), 'name', 'active', 'color')
    file, last_modified_expr = read_landing_fantasy_assets()
    drivers = pl.LazyFrame(file['drivers'])
    drivers = format_raw_df(drivers, drivers_lookup)
    polars_to_parquet(filedir=constants.BRONZE_FANTASY_PATH, filename='drivers', data=drivers)
    drivers_meta = parquet_metadata(f'{constants.BRONZE_FANTASY_PATH}/drivers.parquet').to_dict()
    context.add_output_metadata({'Rows':MetadataValue.int(meta['num_rows'])})
    context.add_output_metadata({'Columns':MetadataValue.int(meta['num_columns'])})