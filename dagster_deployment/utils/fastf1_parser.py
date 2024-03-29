import glob
import json
import polars as pl
import pyarrow.dataset as ds

def parse_json_signals(context, signal_directory: str) -> pl.DataFrame:
    '''Given an asset subdirectory, load all json files for all years and return a polars dataframe.'''
    glob_path = glob.glob(f'data/landing/fastf1/*/{signal_directory}/*.json')
    dfs = [pl.read_json(file) for file in glob_path]
    df = pl.concat(dfs)
    return df

def parse_parquet_signals(context, signal_directory: str) -> pl.DataFrame:
    df = pl.scan_parquet(f'data/landing/fastf1/*/{signal_directory}/*.parquet')
    return df