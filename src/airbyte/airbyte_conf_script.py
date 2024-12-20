"""Call the airbyte API to create sources, destinations and connections for the F1toolbox project.
Requires local credential json files for service account key, hmac key and airbyte api credentials."""

import json
import logging

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

host = "airbyte.f1toolbox.com"
api_url = f"https://{host}/api/public/v1"
workspace_id = "25d4d82d-9a0f-4cf8-92a6-4847b420c982"  # from airbyte UI link


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
            "name": stream,
            "workspaceId": workspace_id,
            "configuration": {
                "sourceType": "gcs",
                "bucket": "f1toolbox-bronze-bucket",
                "credentials": {
                    "auth_type": "Service",
                    "service_account": service_account_key,
                },
                "streams": [
                    {
                        "name": streams[stream],
                        "globs": [f"**{streams[stream]}.parquet"],
                        "validation_policy": "Emit Record",
                        "schemaless": False,
                        "format": {"filetype": "parquet", "decimal_as_float": False},
                    }
                ],
            },
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
                "gcs_bucket_name": "f1toolbox-bronze-bucket",
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
        "prefix": "bq_",
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
    # some streams disabled due to poor airbyte performance, synced directly
    "fantasy_constructor_results": "bronze_fantasy_constructor_results",
    "fantasy_driver_results": "bronze_fantasy_driver_results",
    "fantasy_current_constructors": "bronze_fantasy_current_constructors",
    "fantasy_current_drivers": "bronze_fantasy_current_drivers",
    "fantasy_rounds": "bronze_fantasy_rounds",
    "fastf1_circuit_corners": "bronze_fastf1_circuit_corners",
    "fastf1_events": "bronze_fastf1_events",
    # "fastf1_laps": "bronze_fastf1_laps",
    # "fastf1_session_results": "bronze_fastf1_session_results",
    # "fastf1_sessions": "bronze_fastf1_sessions",
    "fastf1_tyres": "bronze_fastf1_tyres",
    "fastf1_weathers": "bronze_fastf1_weathers",
    # "fastf1_telemetry": "bronze_fastf1_telemetry",
}
delete_resources("sources")
delete_resources("destinations")
delete_resources("connections")
sources = create_sources(streams)
logging.info(f"sources: {sources}")
destination = create_bigquery_destination()
logging.info(f"destination: {destination}")
connections = {}
for source in sources:
    connections.update({source["source_name"]: create_connection(source, destination)})
logging.info(f"connections: {connections}")
with open("src/airbyte/latest_airbyte_connections.json", "w") as f:
    json.dump(connections, f, indent=4)
