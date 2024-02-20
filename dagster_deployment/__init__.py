from dagster import Definitions, load_assets_from_modules
from dagster_deployment.assets import fantasy_results

defs = Definitions(
    assets=load_assets_from_modules([fantasy_results])
)