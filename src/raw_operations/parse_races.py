import os, sys
sys.path.append(os.path.abspath('./'))

import json
import logging
import polars as pl
from src.utils.config import years
from src.utils.iomanager import polars_to_parquet

created=False
for year in years:
    logging.info(f'Year: {year}')
    with open(f'data/landing/fantasy/{year}/races.json', 'r') as f:
        file = json.load(f)
    temp_df = pl.LazyFrame(file['races'])
    temp_df = temp_df.with_columns(pl.lit(year).alias('season'))
    if not created:
        df = temp_df
        created=True
    else:
        df = pl.concat([df, temp_df])

polars_to_parquet(filedir='data/bronze/fantasy', filename='races', data=df)