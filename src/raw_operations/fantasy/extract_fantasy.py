import os, sys
sys.path.append(os.path.abspath('./'))

import json
import logging
import requests
from src.utils.config import years
from src.utils.iomanager import save_raw_json as save_json

BASE_FANTASY_URL = 'https://f1fantasytoolsapi-szumjzgxfa-ew.a.run.app'
FANTASY_ASSETS_ENDPOINT = 'asset-info/init'
DRIVER_RESULTS_ENDPOINT = '/race-results/driver'
CONSTRUCTOR_RESULTS_ENDPOINT = '/race-results/constructor'
RACES_ENDPOINT = '/race-results/races'


def get_request(url:str, params:dict=None):
    resp = requests.get(url, params)
    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if resp.status_code == 404:
            logging.warning(f'No data found for {url}')
            return {}
    resp_dict = resp.json()
    return resp_dict

curr_assets = get_request(f'{BASE_FANTASY_URL}/{FANTASY_ASSETS_ENDPOINT}')
save_json(curr_assets, 'current_fantasy_assets')
for year in years:
    #iterate over the available seasons
    results_params = {
        "season": year
    }
    #make all the needed requests and save them as json files
    driver_results_resp = get_request(f'{BASE_FANTASY_URL}/{DRIVER_RESULTS_ENDPOINT}', params=results_params)
    save_json(driver_results_resp, 'driver_results', year)
    constructor_results_resp = get_request(f'{BASE_FANTASY_URL}/{CONSTRUCTOR_RESULTS_ENDPOINT}', params=results_params)
    save_json(constructor_results_resp, 'constructor_results', year)
    races_resp = get_request(f'{BASE_FANTASY_URL}/{RACES_ENDPOINT}', params=results_params)
    save_json(races_resp, 'races', year)
    
    
    
    
    