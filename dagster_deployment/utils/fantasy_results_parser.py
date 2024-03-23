import os, sys
sys.path.append(os.path.abspath('./'))

import polars as pl
import json

from utils.schemas import season_metadata
from utils.iomanager import polars_to_parquet
from assets.constants import YEARS as years

def parse_results(context, result_type: str) -> pl.DataFrame:
    '''Parse constructor or driver results from https://f1fantasytools.com/race-results, saved in landing zone json files.
    Concatenates all years and returns a polars dataframe.

    arguments:
        result_type: str, 'constructor' or 'driver'

    returns pl.DataFrame
    '''
    year_created=False

    for year in years:
        #These imports happen inside the loop to reset schema contracts override logic which happens year-to-year
        if result_type == 'driver':
            from utils.schemas import driver_schema_contract as schema_contract
            from utils.schemas import driver_schema_contract_no_sprints as schema_contract_no_sprints
        elif result_type == 'constructor':
            from utils.schemas import constructor_schema_contract as schema_contract
            from utils.schemas import constructor_schema_contract_no_sprints as schema_contract_no_sprints
        else:
            raise ValueError('result_type must be either "driver" or "constructor"')
        context.log.info(f'Year: {year}')
        with open(f'data/landing/fantasy/{year}/{result_type}_results.json', 'r') as f:
            file = json.load(f)
        for i in range(season_metadata['latest'][f'{result_type}s']):
            context.log.info(f'{result_type} Number: {i}')
            #Get fantasy points and create dataframe of length of number of races
            fantasy_points = file[i]['race_results'][0]['results_per_race_list']
            fantasy = pl.LazyFrame(fantasy_points, schema=['points_scored'])
            #Get ID and color and assign as literals
            fantasy = fantasy.with_columns(pl.lit(file[i]['abbreviation']).alias('id'))
            fantasy = fantasy.with_columns(pl.lit(file[i]['color']).alias('color'))
            try:
                assert len(file[i]['race_results'][0]['fantasy_results']) == schema_contract['fantasy_results_expectations']['len']
            except AssertionError:
                if not any('sprint' in s['id'] for s in file[i]["race_results"][0]["fantasy_results"]):
                    context.log.warning('No sprint race points received, validating driver_schema_contract_no_sprints instead...')
                    try:
                        assert len(file[i]['race_results'][0]['fantasy_results']) == schema_contract_no_sprints['fantasy_results_expectations']['len']
                        schema_contract = schema_contract_no_sprints
                        context.log.warning(f'A new schema contract without sprint races was succesfully set: {schema_contract}')
                    except AssertionError:
                        context.log.error(f'Fantasy results length mismatch for {result_type} {i} in {year}')
                        raise AssertionError(f'Expected: {schema_contract["fantasy_results_expectations"]}, \n Got: {file[i]["race_results"][0]["fantasy_results"]}')
                else: 
                    context.log.error(f'Expected: {schema_contract["fantasy_results_expectations"]}, \n Got: {file[i]["race_results"][0]["fantasy_results"]}')
                    raise AssertionError()
            #iterate over the fantasy scoring attributes and assign them as named cols
            for y in range(schema_contract['fantasy_results_expectations']['len']):
                try:
                    assert file[i]['race_results'][0]['fantasy_results'][y]['id'] == schema_contract['fantasy_results_expectations']['entries'][y]
                except AssertionError:
                    context.log.error(f"\n Expected: {schema_contract['fantasy_results_expectations']['entries'][y]}, \n Got: {file[i]['race_results'][0]['fantasy_results'][y]['id']}")
                    raise AssertionError()
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
                if schema_contract['fantasy_results_expectations']['entries'] == schema_contract_no_sprints['fantasy_results_expectations']['entries']:
                    context.log.warning('Using polars concat with diagonal to fill empty columns, due to missing sprint race data...')
                    df = pl.concat([df, fantasy], how='diagonal')
                else:
                    df = pl.concat([df, fantasy])
    return df