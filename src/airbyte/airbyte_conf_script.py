"""Call the airbyte API to create sources, destinations and connections for the F1toolbox project.
Requires local credential json files for service account key, hmac key and airbyte api credentials."""

import json

import requests

# load gcp service account key
with open("/home/leo/Downloads/airbyte-key-f1toolbox-core.json", "rb") as f:
    service_account_key = json.dumps(json.load(f))

# load hmac keys for bigquery destination sync
with open("/home/leo/Downloads/hmac_key_airbyte.json", "rb") as f:
    hmac_key_dict = json.load(f)
    hmac_key_id = hmac_key_dict["access_key_id"]
    hmac_key_secret = hmac_key_dict["access_key_secret"]

# load airbyte api credentials
with open("/home/leo/Downloads/airbyte_api_credentials.json", "rb") as f:
    airbyte_credentials = json.load(f)
    client_id = airbyte_credentials["client_id"]
    client_secret = airbyte_credentials["client_secret"]

host = "airbyte.104.154.59.130.nip.io"
api_url = f"http://{host}/api/public/v1"
workspace_id = "d7313f07-aa22-4193-bb4b-da2b40aaa00a"  # from airbyte UI link


def get_token():
    token_endpoint = "applications/token"

    resp = requests.post(
        url=f"{api_url}/{token_endpoint}",
        timeout=60,
        json={
            "client_id": client_id,
            "client_secret": client_secret,
            "grant-type": "client_credentials",
        },
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


# delete all sources (could have been done with pagination and putting instead of delete and recreate but this will not be a regular activity)
def delete_resources(resource_type: str):
    """accepts one of: sources, destinations, connections"""
    keys = {
        "sources": "sourceId",
        "destinations": "destinationId",
        "connections": "connectionId",
    }
    params = {"workspaceId": workspace_id, "limit": 100}
    while True:
        resp = requests.get(
            f"{api_url}/{resource_type}",
            params=params,
            headers={"Authorization": f"Bearer {get_token()}"},
        )
        resp.raise_for_status()
        if not resp.json()["data"]:
            break
        for resource in resp.json()["data"]:
            resource_id = resource[keys[resource_type]]
            resp = requests.delete(
                f"{api_url}/{resource_type}/{resource_id}",
                headers={"Authorization": f"Bearer {get_token()}"},
            )
            resp.raise_for_status()


# create sources
def create_sources(streams: dict) -> list:
    saved_sources = []
    for stream in streams:
        payload = {
            "configuration": {
                "sourceType": "gcs",
                "service_account": service_account_key,
                "bucket": "f1toolbox-bronze-bucket",
                "streams": [
                    {
                        "name": streams[stream],
                        "globs": ["**"],
                        "validation_policy": "Emit Record",
                        "schemaless": False,
                        "format": {"filetype": "parquet", "decimal_as_float": False},
                    }
                ],
            },
            "name": stream,
            "workspaceId": "d7313f07-aa22-4193-bb4b-da2b40aaa00a",
        }
        headers = {"authorization": f"Bearer {get_token()}"}
        response = requests.post(
            url=f"{api_url}/sources", json=payload, headers=headers
        )
        response.raise_for_status()
        saved_sources.append(
            {"source_id": response.json()["sourceId"], "source_name": stream}
        )
    return saved_sources


# create destinations
def create_bigquery_destination() -> str:
    payload = {
        "configuration": {
            "destinationType": "bigquery",
            "project_id": "f1toolbox-core",
            "dataset_location": "us-central1",
            "dataset_id": "f1toolbox_core",
            "loading_method": {
                "method": "GCS Staging",
                "credential": {
                    "credential_type": "HMAC_KEY",
                    "hmac_key_access_id": hmac_key_id,
                    "hmac_key_secret": hmac_key_secret,
                },
                "gcs_bucket_name": "airbyte_state_bucket",
                "gcs_bucket_path": "data_sync/bigquery",
                "keep_files_in_gcs-bucket": "Delete all tmp files from GCS",
            },
            "transformation_priority": "interactive",
            "credentials_json": service_account_key,
        },
        "name": "BigQuery F1toolbox Core",
        "workspaceId": workspace_id,
    }
    headers = {"authorization": f"Bearer {get_token()}"}
    response = requests.post(
        url=f"{api_url}/destinations", json=payload, headers=headers
    )
    response.raise_for_status()
    return {
        "destination_id": response.json()["destinationId"],
        "destination_name": "BigQuery F1toolbox Core",
    }


# create connections
def create_connection(source: dict, destination: dict):
    payload = {
        "name": f"{source['source_name']} -> {destination['destination_name']}",
        "sourceId": source["source_id"],
        "destinationId": destination["destination_id"],
        "configurations": {
            "streams": [
                {
                    "syncMode": "full_refresh_overwrite",
                    "name": f"bronze_{source['source_name']}",
                    #'cursorField': ['_ab_source_file_last_modified'],
                    #'primaryKey': []
                }
            ]
        },
        "dataResidency": "auto",
        "prefix": "",
        "namespaceDefinition": "destination",
        "nonBreakingSchemaUpdatesBehavior": "propagate_fully",
        "schedule": {"scheduleType": "manual"},
    }
    headers = {"authorization": f"Bearer {get_token()}"}
    response = requests.post(
        url=f"{api_url}/connections", json=payload, headers=headers
    )
    response.raise_for_status()
    return response.json()["connectionId"]


# execution
streams = {
    "fantasy_constructor_results": "bronze_fantasy_constructor_results",
    "fantasy_driver_results": "bronze_fantasy_driver_results",
    "fantasy_current_constructors": "bronze_fantasy_current_constructors",
    "fantasy_current_drivers": "bronze_fantasy_current_drivers",
    "fantasy_rounds": "bronze_fantasy_rounds",
    "fastf1_circuit_corners": "bronze_fastf1_circuit_corners",
    "fastf1_events": "bronze_fastf1_events",
    "fastf1_laps": "bronze_fastf1_laps",
    "fastf1_session_results": "bronze_fastf1_session_results",
    "fastf1_sessions": "bronze_fastf1_sessions",
    "fastf1_tyres": "bronze_fastf1_tyres",
    "fastf1_weathers": "bronze_fastf1_weathers",
    "fastf1_telemetry": "bronze_fastf1_telemetry",
}
delete_resources("sources")
delete_resources("destinations")
delete_resources("connections")
sources = create_sources(streams)
destination = create_bigquery_destination()
for source in sources:
    create_connection(source, destination)
