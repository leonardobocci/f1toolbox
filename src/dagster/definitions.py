from dagster_dbt import DbtCliResource

from dagster import (
    AutomationCondition,
    Definitions,
    load_assets_from_modules,
)
from src.dagster.assets import (
    airbyte_assets,
    dbt_assets,
    fantasy_bronze,
    fantasy_landing,
    fastf1_bronze,
    fastf1_landing,
)
from src.dagster.assets.constants import PROJECT
from src.dagster.dbt_project import dbt_project
from src.dagster.jobs import (
    dbt_refresh,
    refresh_fantasy_bronze,
    refresh_fantasy_landing,
    refresh_fastf1_bronze,
    refresh_fastf1_landing,
    refresh_season_fastf1_landing,
)
from src.dagster.resources import airbyte_instance
from src.dagster.utils.iomanager import GcsJsonIoManager, GCSPolarsParquetIOManager

materialization_condition = AutomationCondition.eager()

landing_fantasy_assets = load_assets_from_modules([fantasy_landing])
bronze_fantasy_assets = load_assets_from_modules(
    [fantasy_bronze], automation_condition=materialization_condition
)
all_airbyte_assets = load_assets_from_modules(
    [airbyte_assets], automation_condition=materialization_condition
)
all_dbt_assets = load_assets_from_modules(
    [dbt_assets], automation_condition=materialization_condition
)
landing_fastf1_assets = load_assets_from_modules([fastf1_landing])
bronze_fastf1_assets = load_assets_from_modules(
    [fastf1_bronze], automation_condition=materialization_condition
)

all_jobs = [
    refresh_fantasy_landing,
    refresh_fantasy_bronze,
    refresh_season_fastf1_landing,
    refresh_fastf1_landing,
    refresh_fastf1_bronze,
    dbt_refresh,
]

defs = Definitions(
    assets=[
        *landing_fantasy_assets,
        *bronze_fantasy_assets,
        *landing_fastf1_assets,
        *bronze_fastf1_assets,
        *all_dbt_assets,
        *all_airbyte_assets,
    ],
    jobs=all_jobs,
    resources={
        "airbyte_instance": airbyte_instance,
        "dbt": DbtCliResource(project_dir=dbt_project),
        "gcs_json_fantasy_landing_io_manager": GcsJsonIoManager(
            project=PROJECT,
            bucket_name="f1toolbox-landing-bucket",
            optional_prefix="fantasy",
        ),
        "gcs_json_fastf1_landing_io_manager": GcsJsonIoManager(
            project=PROJECT,
            bucket_name="f1toolbox-landing-bucket",
            optional_prefix="fastf1",
        ),
        "gcs_parquet_fastf1_landing_io_manager": GCSPolarsParquetIOManager(
            project=PROJECT,
            bucket_name="f1toolbox-landing-bucket",
            optional_prefix="fastf1",
        ),
        "gcs_parquet_fantasy_bronze_io_manager": GCSPolarsParquetIOManager(
            project=PROJECT,
            bucket_name="f1toolbox-bronze-bucket",
            optional_prefix="fantasy",
        ),
        "gcs_parquet_fastf1_bronze_io_manager": GCSPolarsParquetIOManager(
            project=PROJECT,
            bucket_name="f1toolbox-bronze-bucket",
            optional_prefix="fastf1",
        ),
    },
)
