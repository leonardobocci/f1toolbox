import requests

from dagster import MetadataValue, asset
from src.dagster.partitions import fantasy_partitions

BASE_FANTASY_URL = "https://f1fantasytoolsapi-szumjzgxfa-ew.a.run.app"
FANTASY_ASSETS_ENDPOINT = "asset-info/init"
DRIVER_RESULTS_ENDPOINT = "/race-results/driver"
CONSTRUCTOR_RESULTS_ENDPOINT = "/race-results/constructor"
RACES_ENDPOINT = "/race-results/races"


def get_request(context, url: str, params: dict = None) -> dict:
    resp = requests.get(url, params)
    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError:
        if resp.status_code == 404:
            context.log.warning(f"No data found for {url}")  # noqa: F821
            return {}
    resp_dict = resp.json()
    return resp_dict


@asset(
    group_name="landing_fantasy_files",
    compute_kind="python",
    io_manager_key="gcs_json_fantasy_landing_io_manager",
)
def landing_fantasy_current_assets(context):
    """Save raw data from f1fantasytools about current driver and constructor prices in landing zone"""
    curr_assets = get_request(context, f"{BASE_FANTASY_URL}/{FANTASY_ASSETS_ENDPOINT}")
    num_constructors = len(curr_assets["constructors"])
    num_drivers = len(curr_assets["drivers"])
    context.add_output_metadata({"Constructors": MetadataValue.int(num_constructors)})
    context.add_output_metadata({"Drivers": MetadataValue.int(num_drivers)})
    return curr_assets


@asset(
    group_name="landing_fantasy_files",
    partitions_def=fantasy_partitions,
    compute_kind="python",
    io_manager_key="gcs_json_fantasy_landing_io_manager",
)
def landing_fantasy_constructor_results(context):
    """Save raw data from f1fantasytools about constructor season fantasy points and prices in landing zone"""
    season = context.partition_key
    results_params = {"season": season}
    constructor_results_resp = get_request(
        context,
        f"{BASE_FANTASY_URL}/{CONSTRUCTOR_RESULTS_ENDPOINT}",
        params=results_params,
    )
    return constructor_results_resp


@asset(
    group_name="landing_fantasy_files",
    partitions_def=fantasy_partitions,
    compute_kind="python",
    io_manager_key="gcs_json_fantasy_landing_io_manager",
)
def landing_fantasy_driver_results(context):
    """Save raw data from f1fantasytools about driver season fantasy points and prices in landing zone"""
    season = context.partition_key
    results_params = {"season": season}
    driver_results_resp = get_request(
        context, f"{BASE_FANTASY_URL}/{DRIVER_RESULTS_ENDPOINT}", params=results_params
    )
    return driver_results_resp


@asset(
    group_name="landing_fantasy_files",
    partitions_def=fantasy_partitions,
    compute_kind="python",
    io_manager_key="gcs_json_fantasy_landing_io_manager",
)
def landing_fantasy_races(context):
    """Save raw data from f1fantasytools about season races in landing zone"""
    season = context.partition_key
    results_params = {"season": season}
    races_resp = get_request(
        context, f"{BASE_FANTASY_URL}/{RACES_ENDPOINT}", params=results_params
    )
    return races_resp
