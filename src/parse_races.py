import json
import polars as pl
from config import years
from iomanager import polars_to_parquet

created=False
for year in years:
    with open(f'data/landing/fantasy/{year}/races.json', 'r') as f:
        file = json.load(f)
    temp_df = pl.LazyFrame(file['races'])
    temp_df = temp_df.with_columns(pl.lit(year).alias('season'))
    if not created:
        df = temp_df
    else:
        df = pl.concat([df, temp_df])

polars_to_parquet(filedir='data/bronze/fantasy', filename='races', data=df)