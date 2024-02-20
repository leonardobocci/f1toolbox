from dagster import Definitions, load_assets_from_modules
from assets import fantasy_results

fantasy_assets = load_assets_from_modules([fantasy_results])

defs = Definitions(
    assets=[*fantasy_assets],
)