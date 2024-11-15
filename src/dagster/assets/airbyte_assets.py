from dagster_airbyte import build_airbyte_assets

from dagster import AssetKey, with_resources
from src.dagster.resources import airbyte_instance

bq_bronze_fantasy_constructor_results = with_resources(
    build_airbyte_assets(
        connection_id="0ed6a2d4-c25b-424e-ab6c-55af5ded1282",
        destination_tables=["bq_bronze_fantasy_constructor_results"],
        deps=[AssetKey("bronze_fantasy_constructor_results")],
        group_name="bronze_bigquery",
    ),
    {"airbyte": airbyte_instance},
)

bq_bronze_fantasy_current_constructors = with_resources(
    build_airbyte_assets(
        connection_id="857a11ab-8ece-4d97-a734-c1650be17364",
        destination_tables=["bq_bronze_fantasy_current_constructors"],
        deps=[AssetKey("bronze_fantasy_current_constructors")],
        group_name="bronze_bigquery",
    ),
    {"airbyte": airbyte_instance},
)

bq_bronze_fantasy_current_drivers = with_resources(
    build_airbyte_assets(
        connection_id="27ba1edd-0bdc-4e81-b4dd-99da617c4eb6",
        destination_tables=["bq_bronze_fantasy_current_drivers"],
        deps=[AssetKey("bronze_fantasy_current_drivers")],
        group_name="bronze_bigquery",
    ),
    {"airbyte": airbyte_instance},
)

bq_bronze_fantasy_driver_results = with_resources(
    build_airbyte_assets(
        connection_id="14c3b431-43ba-4bc8-89ee-92eb7ce65346",
        destination_tables=["bq_bronze_fantasy_driver_results"],
        deps=[AssetKey("bronze_fantasy_driver_results")],
        group_name="bronze_bigquery",
    ),
    {"airbyte": airbyte_instance},
)

bq_bronze_fantasy_rounds = with_resources(
    build_airbyte_assets(
        connection_id="a35c4c26-d6fd-41f1-bb6d-a861d245e875",
        destination_tables=["bq_bronze_fantasy_rounds"],
        deps=[AssetKey("bronze_fantasy_rounds")],
        group_name="bronze_bigquery",
    ),
    {"airbyte": airbyte_instance},
)

bq_bronze_fastf1_circuit_corners = with_resources(
    build_airbyte_assets(
        connection_id="b0277e24-91fa-4c98-be50-c4f6f00d8ba7",
        destination_tables=["bq_bronze_fastf1_circuit_corners"],
        deps=[AssetKey("bronze_fastf1_circuit_corners")],
        group_name="bronze_bigquery",
    ),
    {"airbyte": airbyte_instance},
)

bq_bronze_fastf1_events = with_resources(
    build_airbyte_assets(
        connection_id="347ac1cb-af05-4cf6-93f3-036fce67dc80",
        destination_tables=["bq_bronze_fastf1_events"],
        deps=[AssetKey("bronze_fastf1_events")],
        group_name="bronze_bigquery",
    ),
    {"airbyte": airbyte_instance},
)

bq_bronze_fastf1_laps = with_resources(
    build_airbyte_assets(
        connection_id="b9e16c57-2563-460d-aa20-670bf4c2a47d",
        destination_tables=["bq_bronze_fastf1_laps"],
        deps=[AssetKey("bronze_fastf1_laps")],
        group_name="bronze_bigquery",
    ),
    {"airbyte": airbyte_instance},
)

bq_bronze_fastf1_session_results = with_resources(
    build_airbyte_assets(
        connection_id="4103c778-46c9-4a79-b7d2-7f6187a2e667",
        destination_tables=["bq_bronze_fastf1_session_results"],
        deps=[AssetKey("bronze_fastf1_session_results")],
        group_name="bronze_bigquery",
    ),
    {"airbyte": airbyte_instance},
)

bq_bronze_fastf1_sessions = with_resources(
    build_airbyte_assets(
        connection_id="42c9df5e-3a73-4baa-9d97-3c26847c9664",
        destination_tables=["bq_bronze_fastf1_sessions"],
        deps=[AssetKey("bronze_fastf1_sessions")],
        group_name="bronze_bigquery",
    ),
    {"airbyte": airbyte_instance},
)

bq_bronze_fastf1_tyres = with_resources(
    build_airbyte_assets(
        connection_id="f6635af6-f6c7-4bdd-9fba-4e24b3fce8cb",
        destination_tables=["bq_bronze_fastf1_tyres"],
        deps=[AssetKey("bronze_fastf1_tyres")],
        group_name="bronze_bigquery",
    ),
    {"airbyte": airbyte_instance},
)

bq_bronze_fastf1_weathers = with_resources(
    build_airbyte_assets(
        connection_id="5efa3895-37ed-4d85-b3ce-7ece4953a4d8",
        destination_tables=["bq_bronze_fastf1_weathers"],
        deps=[AssetKey("bronze_fastf1_weathers")],
        group_name="bronze_bigquery",
    ),
    {"airbyte": airbyte_instance},
)
