import glob
import json
import polars as pl
import pyarrow.dataset as ds
from pathlib import Path

def parse_json_signals(context, signal_directory: str) -> pl.DataFrame:
    '''Given an asset subdirectory, load all json files for all years and return a polars dataframe.'''
    glob_path = glob.glob(f'data/landing/fastf1/*/{signal_directory}/*.json')
    dfs = [pl.read_json(file) for file in glob_path]
    df = pl.concat(dfs)
    return df

def parse_parquet_signals(context, signal_directory: str) -> pl.DataFrame:
    if signal_directory == 'laps':
        df = pl.concat([
            pl.scan_parquet(str(x)).with_columns(pl.col('Deleted').cast(pl.Utf8))
            for x in Path('data/landing/fastf1').rglob(f'{signal_directory}/*.parquet')
        ]).collect(streaming=True)
    else:
        df = pl.scan_parquet(f'data/landing/fastf1/*/{signal_directory}/*.parquet')
    return df