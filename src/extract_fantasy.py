import json
import os
import logging
import requests
from config import years
from iomanager import save_raw_json as save_json

BASE_FANTASY_URL = 'https://f1fantasytoolsapi-szumjzgxfa-ew.a.run.app'
FANTASY_ASSETS_ENDPOINT = 'asset-info/init'
DRIVER_RESULTS_ENDPOINT = '/race-results/driver'
CONSTRUCTOR_RESULTS_ENDPOINT = '/race-results/constructor'
RACES_ENDPOINT = '/race-results/races'
LAST_AVAILABLE_RACE_ENDPOINT = '/race-results/last-race'


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

for year in years:
    #iterate over the available seasons
    results_params = {
        "season": year
    }
    #make all the needed requests and save them as json files
    info_resp = get_request(f'{BASE_FANTASY_URL}/{FANTASY_ASSETS_ENDPOINT}')
    save_json(info_resp, year, 'fantasy_assets')
    driver_results_resp = get_request(f'{BASE_FANTASY_URL}/{DRIVER_RESULTS_ENDPOINT}', params=results_params)
    save_json(driver_results_resp, year, 'driver_results')
    constructor_results_resp = get_request(f'{BASE_FANTASY_URL}/{CONSTRUCTOR_RESULTS_ENDPOINT}', params=results_params)
    save_json(constructor_results_resp, year, 'constructor_results')
    races_resp = get_request(f'{BASE_FANTASY_URL}/{RACES_ENDPOINT}', params=results_params)
    save_json(races_resp, year, 'races')
    last_race_resp = get_request(f'{BASE_FANTASY_URL}/{LAST_AVAILABLE_RACE_ENDPOINT}', params=results_params)
    save_json(last_race_resp, year, 'last_race')
    
    
    
    
    