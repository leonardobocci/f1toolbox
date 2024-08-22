import requests

from dagster import MetadataValue, asset
from src.dagster.partitions import fantasy_partitions
from src.dagster.utils.iomanager import save_landing_fantasy_json as save_json

BASE_FANTASY_URL = "https://f1fantasytoolsapi-szumjzgxfa-ew.a.run.app"
FANTASY_ASSETS_ENDPOINT = "asset-info/init"
DRIVER_RESULTS_ENDPOINT = "/race-results/driver"
CONSTRUCTOR_RESULTS_ENDPOINT = "/race-results/constructor"
RACES_ENDPOINT = "/race-results/races"


def get_request(url: str, params: dict = None) -> dict:
    resp = requests.get(url, params)
    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError:
        if resp.status_code == 404:
            context.log.warning(f"No data found for {url}")  # noqa: F821
            return {}
    resp_dict = resp.json()
    return resp_dict


@asset(group_name="landing_fantasy_files", compute_kind="python")
def landing_fantasy_current_assets(context):
    """Save raw data from f1fantasytools about current driver and constructor prices in landing zone"""
    curr_assets = get_request(f"{BASE_FANTASY_URL}/{FANTASY_ASSETS_ENDPOINT}")
    save_json(curr_assets, "current_fantasy_assets")
    num_constructors = len(curr_assets["constructors"])
    num_drivers = len(curr_assets["drivers"])
    context.add_output_metadata({"Constructors": MetadataValue.int(num_constructors)})
    context.add_output_metadata({"Drivers": MetadataValue.int(num_drivers)})
    return


@asset(
    group_name="landing_fantasy_files",
    partitions_def=fantasy_partitions,
    compute_kind="python",
)
def landing_fantasy_constructor_results(context):
    """Save raw data from f1fantasytools about constructor season fantasy points and prices in landing zone"""
    season = context.partition_key
    results_params = {"season": season}
    constructor_results_resp = get_request(
        f"{BASE_FANTASY_URL}/{CONSTRUCTOR_RESULTS_ENDPOINT}", params=results_params
    )
    save_json(constructor_results_resp, "constructor_results", season)
    num_rows = len(constructor_results_resp)
    context.add_output_metadata({"Constructors": MetadataValue.int(num_rows)})
    return


@asset(
    group_name="landing_fantasy_files",
    partitions_def=fantasy_partitions,
    compute_kind="python",
)
def landing_fantasy_driver_results(context):
    """Save raw data from f1fantasytools about driver season fantasy points and prices in landing zone"""
    season = context.partition_key
    results_params = {"season": season}
    driver_results_resp = get_request(
        f"{BASE_FANTASY_URL}/{DRIVER_RESULTS_ENDPOINT}", params=results_params
    )
    save_json(driver_results_resp, "driver_results", season)
    num_rows = len(driver_results_resp)
    context.add_output_metadata({"Drivers": MetadataValue.int(num_rows)})
    return


@asset(
    group_name="landing_fantasy_files",
    partitions_def=fantasy_partitions,
    compute_kind="python",
)
def landing_fantasy_races(context):
    """Save raw data from f1fantasytools about season races in landing zone"""
    season = context.partition_key
    results_params = {"season": season}
    races_resp = get_request(
        f"{BASE_FANTASY_URL}/{RACES_ENDPOINT}", params=results_params
    )
    save_json(races_resp, "races", season)
    num_rows = len(races_resp["races"])
    context.add_output_metadata({"Races": MetadataValue.int(num_rows)})
    return
