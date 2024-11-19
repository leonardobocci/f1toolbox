import json

from dagster_airbyte import build_airbyte_assets

from dagster import AssetKey, with_resources
from src.dagster.resources import airbyte_instance

with open("src/airbyte/latest_airbyte_connections.json", "r") as f:
    airbyte_connections = json.load(f)

bq_bronze_fantasy_constructor_results = with_resources(
    build_airbyte_assets(
        connection_id=airbyte_connections["fantasy_constructor_results"],
        destination_tables=["bq_bronze_fantasy_constructor_results"],
        deps=[AssetKey("bronze_fantasy_constructor_results")],
        group_name="bronze_bigquery",
    ),
    {"airbyte": airbyte_instance},
)

bq_bronze_fantasy_current_constructors = with_resources(
    build_airbyte_assets(
        connection_id=airbyte_connections["fantasy_current_constructors"],
        destination_tables=["bq_bronze_fantasy_current_constructors"],
        deps=[AssetKey("bronze_fantasy_current_constructors")],
        group_name="bronze_bigquery",
    ),
    {"airbyte": airbyte_instance},
)

bq_bronze_fantasy_current_drivers = with_resources(
    build_airbyte_assets(
        connection_id=airbyte_connections["fantasy_current_drivers"],
        destination_tables=["bq_bronze_fantasy_current_drivers"],
        deps=[AssetKey("bronze_fantasy_current_drivers")],
        group_name="bronze_bigquery",
    ),
    {"airbyte": airbyte_instance},
)

bq_bronze_fantasy_driver_results = with_resources(
    build_airbyte_assets(
        connection_id=airbyte_connections["fantasy_driver_results"],
        destination_tables=["bq_bronze_fantasy_driver_results"],
        deps=[AssetKey("bronze_fantasy_driver_results")],
        group_name="bronze_bigquery",
    ),
    {"airbyte": airbyte_instance},
)

bq_bronze_fantasy_rounds = with_resources(
    build_airbyte_assets(
        connection_id=airbyte_connections["fantasy_rounds"],
        destination_tables=["bq_bronze_fantasy_rounds"],
        deps=[AssetKey("bronze_fantasy_rounds")],
        group_name="bronze_bigquery",
    ),
    {"airbyte": airbyte_instance},
)

bq_bronze_fastf1_circuit_corners = with_resources(
    build_airbyte_assets(
        connection_id=airbyte_connections["fastf1_circuit_corners"],
        destination_tables=["bq_bronze_fastf1_circuit_corners"],
        deps=[AssetKey("bronze_fastf1_circuit_corners")],
        group_name="bronze_bigquery",
    ),
    {"airbyte": airbyte_instance},
)

bq_bronze_fastf1_events = with_resources(
    build_airbyte_assets(
        connection_id=airbyte_connections["fastf1_events"],
        destination_tables=["bq_bronze_fastf1_events"],
        deps=[AssetKey("bronze_fastf1_events")],
        group_name="bronze_bigquery",
    ),
    {"airbyte": airbyte_instance},
)

bq_bronze_fastf1_tyres = with_resources(
    build_airbyte_assets(
        connection_id=airbyte_connections["fastf1_tyres"],
        destination_tables=["bq_bronze_fastf1_tyres"],
        deps=[AssetKey("bronze_fastf1_tyres")],
        group_name="bronze_bigquery",
    ),
    {"airbyte": airbyte_instance},
)

bq_bronze_fastf1_weathers = with_resources(
    build_airbyte_assets(
        connection_id=airbyte_connections["fastf1_weathers"],
        destination_tables=["bq_bronze_fastf1_weathers"],
        deps=[AssetKey("bronze_fastf1_weathers")],
        group_name="bronze_bigquery",
    ),
    {"airbyte": airbyte_instance},
)
