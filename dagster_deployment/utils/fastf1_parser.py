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
    glob_path = glob.glob(f'data/landing/fastf1/*/{signal_directory}/*.json')
    dfs = [pl.scan_pyarrow_dataset(ds.dataset(file)) for file in glob_path]
    df = pl.concat(dfs, how='vertical_relaxed')
    return df