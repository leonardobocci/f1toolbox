from dagster import Definitions, load_assets_from_modules
from assets import fantasy_raw

fantasy_assets = load_assets_from_modules([fantasy_raw])

defs = Definitions(
    assets=[*fantasy_assets],
)