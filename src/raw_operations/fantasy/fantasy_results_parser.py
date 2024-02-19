import os, sys
sys.path.append(os.path.abspath('./'))

import polars as pl
import json
import logging

from src.utils.config import years, season_metadata
from src.utils.iomanager import polars_to_parquet

def parse_results(result_type: str) -> pl.DataFrame:
    '''Parse constructor or driver results from https://f1fantasytools.com/race-results

    arguments:
        result_type: str, 'constructor' or 'driver'

    returns pl.DataFrame
    '''
    year_created=False
    if result_type == 'driver':
        from src.utils.config import driver_schema_contract as schema_contract
    elif result_type == 'constructor':
        from src.utils.config import constructor_schema_contract as schema_contract
    else:
        raise ValueError('result_type must be either "driver" or "constructor"')

    for year in years:
        logging.info(f'Year: {year}')
        with open(f'data/landing/fantasy/{year}/{result_type}_results.json', 'r') as f:
            file = json.load(f)
        for i in range(season_metadata['latest'][f'{result_type}s']):
            logging.info(f'{result_type} Number: {i}')
            #Get fantasy points and create dataframe of length of number of races
            fantasy_points = file[i]['race_results'][0]['results_per_race_list']
            fantasy = pl.LazyFrame(fantasy_points, schema=['points_scored'])
            #Get ID and assign it as literal
            fantasy = fantasy.with_columns(pl.lit(file[i]['abbreviation']).alias('id'))
            assert len(file[i]['race_results'][0]['fantasy_results']) == schema_contract['fantasy_results_expectations']['len']
            #iterate over the fantasy scoring attributes and assign them as named cols
            for y in range(schema_contract['fantasy_results_expectations']['len']):
                assert file[i]['race_results'][0]['fantasy_results'][y]['id'] == schema_contract['fantasy_results_expectations']['entries'][y]
                fantasy_attribute_list = file[i]['race_results'][0]['fantasy_results'][y]['points_per_race_list']
                #add named columns with fantasy attributes to temporary frame
                fantasy = fantasy.with_columns(pl.Series(name=schema_contract['fantasy_results_expectations']['entries'][y], values=fantasy_attribute_list).cast(pl.Float64))
            try:
                #Will not work for 2022, price data is not available
                assert file[i]['race_results'][1]['id'] == 'price_at_lock'
                price_list = file[i]['race_results'][1]['results_per_race_list']
                fantasy = fantasy.with_columns(pl.Series(name='price', values=price_list).cast(pl.Float64))
            except (AssertionError, IndexError):
                fantasy = fantasy.with_columns(pl.Series(name='price', values=[None]).cast(pl.Float64))
            try:
                #Will not work for 2022, price change data is not available
                assert file[i]['race_results'][2]['id'] == 'price_change'
                price_change_list = file[i]['race_results'][1]['results_per_race_list']
                fantasy = fantasy.with_columns(pl.Series(name='price_change', values=price_change_list).cast(pl.Float64))
            except (AssertionError, IndexError):
                fantasy = fantasy.with_columns(pl.Series(name='price_change', values=[None]).cast(pl.Float64))
            fantasy = fantasy.with_columns(pl.lit(year).alias('season'))
            fantasy = fantasy.with_columns(pl.Series(name="round_number", values=[i+1 for i in range(fantasy.select(pl.len()).collect().item())]))
        
            #concatenate all the years
            if not year_created:
                df = fantasy
                year_created = True
            else:
                df = pl.concat([df, fantasy])
    return df