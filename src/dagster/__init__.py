import os

from dagster import (
    AutoMaterializePolicy,
    AutoMaterializeRule,
    Definitions,
    load_assets_from_modules,
)
from dagster_dbt import DbtCliResource

from src.dagster.assets import (
    fantasy_bronze,
    fantasy_landing,
    fantasy_silver,
    fastf1_bronze,
    fastf1_landing,
)
from src.dagster.assets.constants import dbt_project_dir
from src.dagster.jobs import landing_fantasy_full_job, landing_fastf1_full_job

# Downstream layers are auto materialized whenever the upstream layer is materialized
materialization_policy = AutoMaterializePolicy.eager().with_rules(
    AutoMaterializeRule.skip_on_not_all_parents_updated(
        require_update_for_all_parent_partitions=True
    )  # wait for all parents
)

landing_fantasy_assets = load_assets_from_modules([fantasy_landing])
bronze_fantasy_assets = load_assets_from_modules(
    [fantasy_bronze], auto_materialize_policy=materialization_policy
)
silver_fantasy_assets = load_assets_from_modules(
    [fantasy_silver], auto_materialize_policy=materialization_policy
)

landing_fastf1_assets = load_assets_from_modules([fastf1_landing])
bronze_fastf1_assets = load_assets_from_modules(
    [fastf1_bronze], auto_materialize_policy=materialization_policy
)

all_jobs = [landing_fastf1_full_job, landing_fantasy_full_job]

defs = Definitions(
    assets=[
        *landing_fantasy_assets,
        *bronze_fantasy_assets,
        *landing_fastf1_assets,
        *bronze_fastf1_assets,
        *silver_fantasy_assets,
    ],
    jobs=all_jobs,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    },
)
