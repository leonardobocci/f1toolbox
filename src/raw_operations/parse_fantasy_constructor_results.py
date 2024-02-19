import os, sys
sys.path.append(os.path.abspath('./'))

import pandas as pd
import polars as pl
import json
import logging

from src.utils.config import years, constructor_schema_contract
from src.utils.iomanager import polars_to_parquet

year_created=False
constructor_created=False

'''Received schema:
[{}] response: list of 10 dictionaries (one per constructor) [ {}, {} ]
    abbreviation: str,
    color: str,
    constructor : bool,
    [{}] race_results: list of 2 to 4 dicts (one dict per result type) [ {}, {} ]
        [0] [{}, {}] 
            "fantasy_results": 
                list [ 17 dicts: {'id': 'attr1', 'points_per_race_list': [X0, X1, ... Xlastrace] }, {}],
            "id": "weekend_current_points",
            "results_per_aggregation_list": [],
            "results_per_race_list": [X0, X1, ... Xlastrace]
        [1] (2023+) {} price_at_lock dict:
            dict {'id': 'price_at_lock', 'results_per_aggregation_list': [], 'results_per_race_list': [X0, X1, ... Xlastrace]},
        [2 or 1(2022)] {} price_change:
            dict {'id': 'price_change', 'results_per_aggregation_list': [], 'results_per_race_list': [X0, X1, ... Xlastrace]},
        [3] (2023+) weekend_PPM:
            dict {'id': 'weekend_PPM', 'results_per_aggregation_list': [], 'results_per_race_list': [X0, X1, ... Xlastrace]}

'''

for i in range(10): #10 f1 constructors
    logging.info(f'Constructor Number: {i}')
    for year in years:
        logging.info(f'Year: {year}')
        if year==2022:
            #marker used for 2022 different schema
            small_load = 'small_load'
        else:
            small_load = ''
        with open(f'data/landing/fantasy/{year}/constructor_results.json', 'r') as f:
            file = json.load(f)
        #Schema validation
        assert list(file[i].keys()) == constructor_schema_contract['base_keys']
        assert len(file[i]['race_results']) == len(constructor_schema_contract[f'{small_load}race_result_list_entries'])
        #Get constructor ID
        constructor_id = file[i]['abbreviation']
        #list position tracks 1 to 4 list items inside race_results
        list_position = 0
        #Get constructor total fantasy points after race - list of all races in the season
        assert file[i]['race_results'][list_position]['id'] == constructor_schema_contract['fantasy_results_expectations']['id']
        constructor_total_fantasy_points_list = file[i]['race_results'][list_position]['results_per_race_list']
        #Get constructor fantasy event details - dicts inside fantasy results list
        base_file = file[i]['race_results'][list_position]['fantasy_results']
        assert len(base_file) == constructor_schema_contract['fantasy_results_expectations']['len'] #17 entries expected
        sublist_position = 0
        assert base_file[sublist_position]['id'] == 'quali_not_classified_points'
        quali_not_classified_points = base_file[sublist_position]['points_per_race_list']
        sublist_position +=1
        assert base_file[sublist_position]['id'] == 'quali_disqualified_points'
        quali_disqualified_points = base_file[sublist_position]['points_per_race_list']
        sublist_position +=1
        assert base_file[sublist_position]['id'] == 'quali_pos_points'
        quali_pos_points = base_file[sublist_position]['points_per_race_list']
        sublist_position +=1
        assert base_file[sublist_position]['id'] == 'quali_teamwork_points'
        quali_teamwork_points = base_file[sublist_position]['points_per_race_list']
        sublist_position +=1
        assert base_file[sublist_position]['id'] == 'sprint_pos_gained_points'
        sprint_pos_gained_points = base_file[sublist_position]['points_per_race_list']
        sublist_position +=1
        assert base_file[sublist_position]['id'] == 'sprint_overtake_points'
        sprint_overtake_points = base_file[sublist_position]['points_per_race_list']
        sublist_position +=1
        assert base_file[sublist_position]['id'] == 'sprint_fastest_lap_points'
        sprint_fastest_lap_points = base_file[sublist_position]['points_per_race_list']
        sublist_position +=1
        assert base_file[sublist_position]['id'] == 'sprint_not_classified_points'
        sprint_not_classified_points = base_file[sublist_position]['points_per_race_list']
        sublist_position +=1
        assert base_file[sublist_position]['id'] == 'sprint_disqualified_points'
        sprint_disqualified_points = base_file[sublist_position]['points_per_race_list']
        sublist_position +=1
        assert base_file[sublist_position]['id'] == 'sprint_pos_points'
        sprint_pos_points = base_file[sublist_position]['points_per_race_list']
        sublist_position +=1
        assert base_file[sublist_position]['id'] == 'race_pos_gained_points'
        race_pos_gained_points = base_file[sublist_position]['points_per_race_list']
        sublist_position +=1
        assert base_file[sublist_position]['id'] == 'race_overtake_points'
        race_overtake_points = base_file[sublist_position]['points_per_race_list']
        sublist_position +=1
        assert base_file[sublist_position]['id'] == 'race_fastest_lap_points'
        race_fastest_lap_points = base_file[sublist_position]['points_per_race_list']
        sublist_position +=1
        assert base_file[sublist_position]['id'] == 'race_not_classified_points'
        race_not_classified_points = base_file[sublist_position]['points_per_race_list']
        sublist_position +=1
        assert base_file[sublist_position]['id'] == 'race_disqualified_points'
        race_disqualified_points = base_file[sublist_position]['points_per_race_list']
        sublist_position +=1
        assert base_file[sublist_position]['id'] == 'race_pos_points'
        race_pos_points = base_file[sublist_position]['points_per_race_list']
        sublist_position +=1
        assert base_file[sublist_position]['id'] == 'race_pit_stop_points'
        race_pit_stop_points = base_file[sublist_position]['points_per_race_list']

        if small_load:
            constructor_price_change_list = [None]
            constructor_price_list = [None]
        else:
            #Get constructor price before race - list of all races in the season
            list_position = 1
            assert file[i]['race_results'][list_position]['id'] == constructor_schema_contract[f'{small_load}race_result_list_entries'][list_position]
            constructor_price_list = file[i]['race_results'][list_position]['results_per_race_list']
            #These would cause list out of range for 2022
            #Get constructor price change before race - list of all races in the season
            list_position = 2
            assert file[i]['race_results'][list_position]['id'] == constructor_schema_contract[f'{small_load}race_result_list_entries'][list_position]
            constructor_price_change_list = file[i]['race_results'][list_position]['results_per_race_list']
            #Points per million - not useful as it's a calculated field
            list_position = 3
            assert file[i]['race_results'][list_position]['id'] == constructor_schema_contract[f'{small_load}race_result_list_entries'][list_position]
        #Build out polars dataframe
        constructor_fantasy = pl.LazyFrame(constructor_total_fantasy_points_list, schema=['points_scored'])
        constructor_fantasy = constructor_fantasy.with_columns(pl.Series(name="price", values=constructor_price_list).cast(pl.Float64))
        constructor_fantasy = constructor_fantasy.with_columns(pl.Series(name="price_change", values=constructor_price_change_list).cast(pl.Float64))
        constructor_fantasy = constructor_fantasy.with_columns(pl.lit(constructor_id).alias('id'))
        constructor_fantasy = constructor_fantasy.with_columns(pl.lit(year).alias('season'))
        #Assign round number to each row - data is ordered for now
        constructor_fantasy = constructor_fantasy.with_columns(pl.Series(name="round_number", values=[i+1 for i in range(constructor_fantasy.select(pl.len()).collect().item())]))

        #concatenate all the years
        if not year_created:
            temp_df = constructor_fantasy
            year_created = True
        else:
            temp_df = pl.concat([temp_df, constructor_fantasy])
    
    #concatenate all the constructors
    if not constructor_created:
        df = temp_df
        constructor_created = True
    else:
        df = pl.concat([df, temp_df], how="vertical_relaxed")

polars_to_parquet(filedir='data/bronze/fantasy', filename='constructor_fantasy_attributes', data=df)