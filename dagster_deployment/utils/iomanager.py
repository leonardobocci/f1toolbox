import os
import json
import logging
import polars as pl
from typing import Union
from assets import constants

def save_raw_json(data:dict, filename:str, year:int=None):
    '''Save the raw data extracted from the Fantasy APIs as json files'''
    if data:
        if year:
            raw_fantasy_dir = f'{constants.RAW_FANTASY_PATH}/{year}'
        else:
            raw_fantasy_dir = f'{constants.RAW_FANTASY_PATH}/'
        filepath = os.path.join(raw_fantasy_dir, f'{filename}.json')
        os.makedirs(raw_fantasy_dir, exist_ok=True)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    else:
        context.log.warning(f'Empty dict received for {year} - {filename}')
    return

def polars_to_parquet(filedir:str, filename:str, data:Union[pl.DataFrame, pl.LazyFrame]):
    '''Write a polars frame to parquet file'''
    os.makedirs(filedir, exist_ok=True)
    if isinstance(data, pl.DataFrame):
        data.write_parquet(f'{constants.BRONZE_FANTASY_PATH}{filedir}/{filename}.parquet')
    elif isinstance(data, pl.LazyFrame):
        #streaming not supported for all operations data.sink_parquet(f'{filedir}/{filename}.parquet')
        data.collect().write_parquet(f'{filedir}/{filename}.parquet')
    else:
        raise NotImplementedError('Data type not supported')
    return