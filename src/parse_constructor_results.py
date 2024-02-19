import pandas as pd
import polars as pl
import json
from config import years
from iomanager import polars_to_parquet

year_created=False
constructor_created=False

'''Received schema:
>=2023:
response: list of 10 dictionaries (one per constructor)
dict keys in each element of the list: 
    abbreviation: str,
    color: str,
    constructor : bool,
    race_results: list [
        fantasy_results: list [ 17 fantasy points scoring attributes] ],
        price_at_lock: 

'''

for i in range(10): #10 f1 constructors
    for year in years:
        with open(f'data/landing/fantasy/{year}/constructor_results.json', 'r') as f:
            file = json.load(f)
        #Schema validation
        assert list(file[0].keys()) == ['abbreviation', 'color', 'constructor', 'race_results']
        if year == 2022:
            #in 2022 prices and ppm are not available
            assert len(file[0]['race_results']) == 2
            small_load = True
        else:  
            assert len(file[0]['race_results']) == 4
        #Get constructor ID
        constructor_id = file[0]['abbreviation']
        #Get constructor total fantasy points after race - list of all races in the season
        list_position = 0
        assert file[0]['race_results'][list_position]['id'] == 'weekend_current_points'
        constructor_total_fantasy_points_list = file[0]['race_results'][list_position]['results_per_race_list']
        #Get constructor fantasy event details
        base_file = file[0]['race_results'][list_position]['fantasy_results']
        assert len(base_file) == 17 #list with 17 entries
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
        #Get constructor price before race - list of all races in the season
        list_position = 1
        assert file[0]['race_results'][list_position]['id'] == 'price_at_lock'
        constructor_price_list = file[0]['race_results'][list_position]['results_per_race_list']
        #Get constructor price change before race - list of all races in the season
        list_position = 2
        assert file[0]['race_results'][list_position]['id'] == 'price_change'
        constructor_price_change_list = file[0]['race_results'][list_position]['results_per_race_list']
        #Points per million - not useful as it's a calculated field
        list_position = 3
        assert file[0]['race_results'][list_position]['id'] == 'weekend_PPM'
        #Build out polars dataframe
        constructor_fantasy = pl.LazyFrame(constructor_total_fantasy_points_list, schema=['points_scored'])
        constructor_fantasy = constructor_fantasy.with_columns(pl.Series(name="price", values=constructor_price_list))
        constructor_fantasy = constructor_fantasy.with_columns(pl.Series(name="price_change", values=constructor_price_change_list))
        constructor_fantasy = constructor_fantasy.with_columns(pl.lit(constructor_id).alias('id'))
        constructor_fantasy = constructor_fantasy.with_columns(pl.lit(year).alias('season'))
        #Assign round number to each row
        constructor_fantasy = constructor_fantasy.with_columns(pl.Series(name="round_number", values=[i+1 for i in range(len(constructor_fantasy))]))

        #concatenate all the years
        if not year_created:
            temp_df = constructor_fantasy
            year_created = True
        else:
            temp_df = pl.concat([df, constructor_fantasy])
    
    #concatenate all the constructors
    if not constructor_created:
        df = temp_df
        constructor_created = True
    else:
        df = pl.concat([df, temp_df])

polars_to_parquet(filedir='data/bronze/fantasy', filename='constructor_fantasy_attributes', data=df)