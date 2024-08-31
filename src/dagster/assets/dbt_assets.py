from dagster_dbt import DbtCliResource, dbt_assets

from dagster import AssetExecutionContext
from src.dagster.dbt_project import dbt_project


@dbt_assets(manifest=dbt_project.manifest_path)
def f1toolbox_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from (
        dbt.cli(
            ["build"], context=context
        )  # build creates the dbt model and tests them, run only creates them
        .stream()
        .fetch_column_metadata()
        .fetch_row_counts()
    )
