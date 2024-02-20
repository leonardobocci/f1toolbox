from dagster import Definitions, load_assets_from_modules
from assets import fantasy_landing

fantasy_assets = load_assets_from_modules([fantasy_landing])

defs = Definitions(
    assets=[*fantasy_assets],
)