import os, sys
sys.path.append(os.path.abspath('./'))

import polars as pl
import json
import logging

from src.utils.config import years, driver_schema_contract
from src.utils.iomanager import polars_to_parquet

year_created=False

'''Received schema:
[{}] response: list of 20 dictionaries (one per driver) [ {}, {} ]
    abbreviation: str,
    color: str,
    constructor : bool,
    [{}] race_results: list of 16 dicts (one dict per result type) [ {}, {} ]
        [0] [{}, {}] 
            "fantasy_results": 
                list [ 16 dicts: {'id': 'attr1', 'points_per_race_list': [X0, X1, ... Xlastrace] }, {}],
            "id": "weekend_current_points",
            "results_per_aggregation_list": [],
            "results_per_race_list": [X0, X1, ... Xlastrace]
        [1] (2023+) {} price_at_lock dict:
            dict {'id': 'price_at_lock', 'results_per_aggregation_list': [], 'results_per_race_list': [X0, X1, ... Xlastrace]},
        [2] (2023+)] {} price_change:
            dict {'id': 'price_change', 'results_per_aggregation_list': [], 'results_per_race_list': [X0, X1, ... Xlastrace]},
        [3] (2023+) weekend_PPM:
            dict {'id': 'weekend_PPM', 'results_per_aggregation_list': [], 'results_per_race_list': [X0, X1, ... Xlastrace]}
        [4-15] Not Required - Actual race results, not fantasy-related
'''

for year in years:
    logging.info(f'Year: {year}')
    with open(f'data/landing/fantasy/{year}/driver_results.json', 'r') as f:
        file = json.load(f)
    for i in range(20): #20 f1 drivers
        logging.info(f'Driver Number: {i}')
        #Get driver fantasy points and create dataframe of length of number of races
        driver_fantasy_points = file[i]['race_results'][0]['results_per_race_list']
        driver_fantasy = pl.LazyFrame(driver_fantasy_points, schema=['points_scored'])
        #Get driver ID and assign it as literal
        driver_fantasy = driver_fantasy.with_columns(pl.lit(file[i]['abbreviation']).alias('id'))
        assert len(file[i]['race_results'][0]['fantasy_results']) == driver_schema_contract['fantasy_results_expectations']['len']
        #iterate over the fantasy scoring attributes and assign them as named cols
        for y in range(15):
            assert file[i]['race_results'][0]['fantasy_results'][y]['id'] == driver_schema_contract['fantasy_results_expectations']['entries'][y]
            fantasy_attribute_list = file[i]['race_results'][0]['fantasy_results'][y]['points_per_race_list']
            #add named columns with fantasy attributes to temporary frame
            driver_fantasy = driver_fantasy.with_columns(pl.Series(name=driver_schema_contract['fantasy_results_expectations']['entries'][y], values=fantasy_attribute_list).cast(pl.Float64))
        #iterate over the pricing data and assign them as named cols
        try:
            #Will not work for 2022, price data is not available
            assert file[i]['race_results'][1]['id'] == 'price_at_lock'
            price_list = file[i]['race_results'][1]['results_per_race_list']
            driver_fantasy = driver_fantasy.with_columns(pl.Series(name='price', values=price_list).cast(pl.Float64))
        except AssertionError:
            driver_fantasy = driver_fantasy.with_columns(pl.Series(name='price', values=[None]).cast(pl.Float64))
        try:
            #Will not work for 2022, price change data is not available
            assert file[i]['race_results'][2]['id'] == 'price_change'
            price_change_list = file[i]['race_results'][1]['results_per_race_list']
            driver_fantasy = driver_fantasy.with_columns(pl.Series(name='price_change', values=price_change_list).cast(pl.Float64))
        except AssertionError:
            driver_fantasy = driver_fantasy.with_columns(pl.Series(name='price_change', values=[None]).cast(pl.Float64))
        driver_fantasy = driver_fantasy.with_columns(pl.lit(year).alias('season'))
        driver_fantasy = driver_fantasy.with_columns(pl.Series(name="round_number", values=[i+1 for i in range(driver_fantasy.select(pl.len()).collect().item())]))

    #concatenate all the years
    if not year_created:
        df = driver_fantasy
        year_created = True
    else:
        df = pl.concat([df, driver_fantasy])

polars_to_parquet(filedir='data/bronze/fantasy', filename='driver_fantasy_attributes', data=df)