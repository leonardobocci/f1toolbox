import os, sys
sys.path.append(os.path.abspath('./'))

import json
import logging
import polars as pl
from src.utils.config import years
from src.utils.iomanager import polars_to_parquet

created=False
constructor_lookup = pl.scan_csv('src/utils/constructor_mapping.csv')
drivers_lookup = pl.scan_csv('src/utils/driver_mapping.csv')
unique_constructor_list = pl.scan_parquet('data/bronze/fantasy/constructor_fantasy_attributes.parquet').select('id', 'color').unique()
unique_driver_list = pl.scan_parquet('data/bronze/fantasy/driver_fantasy_attributes.parquet').select('id', 'color').unique()
constructor_lookup = unique_constructor_list.join(constructor_lookup, on='id', how='outer')
drivers_lookup = unique_driver_list.join(drivers_lookup, on='id', how='outer')
constructor_lookup = constructor_lookup.select(pl.coalesce('id', 'id_right').alias('id'), 'name', 'active', 'color')
drivers_lookup = drivers_lookup.select(pl.coalesce('id', 'id_right').alias('id'), 'name', 'active', 'color')

with open(f'data/landing/fantasy/current_fantasy_assets.json', 'r') as f:
    file = json.load(f)
last_modified = os.path.getmtime('data/landing/fantasy/current_fantasy_assets.json')
last_modified_expr = pl.from_epoch(pl.lit(last_modified))

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

constructors = pl.LazyFrame(file['constructors'])
drivers = pl.LazyFrame(file['drivers'])

constructors = format_raw_df(constructors, constructor_lookup)
drivers = format_raw_df(drivers, drivers_lookup)

polars_to_parquet(filedir='data/bronze/fantasy', filename='constructors', data=constructors)
polars_to_parquet(filedir='data/bronze/fantasy', filename='drivers', data=drivers)