import os, sys
sys.path.append(os.path.abspath('./'))

import json
import logging
import requests
from dagster import asset, MetadataValue
from dagster_deployment.partitions import fantasy_partitions
from src.utils.config import years
from src.utils.iomanager import save_raw_json as save_json

BASE_FANTASY_URL = 'https://f1fantasytoolsapi-szumjzgxfa-ew.a.run.app'
FANTASY_ASSETS_ENDPOINT = 'asset-info/init'
DRIVER_RESULTS_ENDPOINT = '/race-results/driver'
CONSTRUCTOR_RESULTS_ENDPOINT = '/race-results/constructor'
RACES_ENDPOINT = '/race-results/races'

def get_request(url:str, params:dict=None) -> dict:
    resp = requests.get(url, params)
    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if resp.status_code == 404:
            logging.warning(f'No data found for {url}')
            return {}
    resp_dict = resp.json()
    return resp_dict

@asset(
    group_name='raw_fantasy_files'
)
def current_fantasy_assets(context):
    curr_assets = get_request(f'{BASE_FANTASY_URL}/{FANTASY_ASSETS_ENDPOINT}')
    save_json(curr_assets, 'current_fantasy_assets')
    num_constructors = len(curr_assets['constructors'])
    num_drivers = len(curr_assets['drivers'])
    context.add_output_metadata({'Constructors':MetadataValue.int(num_constructors)})
    context.add_output_metadata({'Drivers':MetadataValue.int(num_drivers)})
    return

@asset(
    group_name='raw_fantasy_files',
    partitions_def=fantasy_partitions
)
def fantasy_constructor_results(context):
    year_partition = context.asset_partition_key_for_output()
    results_params = {
        "season": year_partition[0]
    }
    constructor_results_resp = get_request(f'{BASE_FANTASY_URL}/{CONSTRUCTOR_RESULTS_ENDPOINT}', params=results_params)
    save_json(constructor_results_resp, 'constructor_results', year)
    num_rows = len(constructor_results_resp)
    context.add_output_metadata({'Constructors':MetadataValue.int(num_rows)})
    return

@asset(
    group_name='raw_fantasy_files',
    partitions_def=fantasy_partitions
)
def fantasy_driver_results(context):
    year_partition = context.asset_partition_key_for_output()
    results_params = {
        "season": year_partition[0]
    }
    driver_results_resp = get_request(f'{BASE_FANTASY_URL}/{DRIVER_RESULTS_ENDPOINT}', params=results_params)
    save_json(driver_results_resp, 'driver_results', year)
    num_rows = len(driver_results_resp)
    context.add_output_metadata({'Drivers':MetadataValue.int(num_rows)})
    return

@asset(
    group_name='raw_fantasy_files',
    partitions_def=fantasy_partitions
)
def fantasy_races(context):
    year_partition = context.asset_partition_key_for_output()
    results_params = {
        "season": year_partition[0]
    }
    races_resp = get_request(f'{BASE_FANTASY_URL}/{RACES_ENDPOINT}', params=results_params)
    save_json(races_resp, 'races', year)
    num_rows = len(races_resp['races'])
    context.add_output_metadata({'Races':MetadataValue.int(num_rows)})
    return
    
    
    
    
    