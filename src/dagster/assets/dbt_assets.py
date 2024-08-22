from dagster_dbt import DbtCliResource, dbt_assets

from dagster import AssetExecutionContext
from src.dagster.assets.constants import dbt_manifest_path


@dbt_assets(manifest=dbt_manifest_path)
def f1toolbox_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream().fetch_column_metadata()
