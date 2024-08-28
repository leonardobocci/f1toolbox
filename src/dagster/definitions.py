from dagster_dbt import DbtCliResource

from dagster import (
    AutomationCondition,
    Definitions,
    load_assets_from_modules,
)
from src.dagster.assets import (
    dbt_assets,
    fantasy_bronze,
    fantasy_landing,
    fastf1_bronze,
    fastf1_landing,
)
from src.dagster.dbt_project import dbt_project
from src.dagster.jobs import landing_fantasy_full_job, landing_fastf1_full_job

materialization_condition = AutomationCondition.eager()  # TODO: figure out how to add all parent partition materialized requirement to prevent trigger on eahc partition load.

landing_fantasy_assets = load_assets_from_modules([fantasy_landing])
bronze_fantasy_assets = load_assets_from_modules(
    [fantasy_bronze], automation_condition=materialization_condition
)
all_dbt_assets = load_assets_from_modules(
    [dbt_assets], automation_condition=materialization_condition
)
landing_fastf1_assets = load_assets_from_modules([fastf1_landing])
bronze_fastf1_assets = load_assets_from_modules(
    [fastf1_bronze], automation_condition=materialization_condition
)

all_jobs = [landing_fastf1_full_job, landing_fantasy_full_job]

defs = Definitions(
    assets=[
        *landing_fantasy_assets,
        *bronze_fantasy_assets,
        *landing_fastf1_assets,
        *bronze_fastf1_assets,
        *all_dbt_assets,
    ],
    jobs=all_jobs,
    resources={
        "dbt": DbtCliResource(project_dir=dbt_project),
    },
)
