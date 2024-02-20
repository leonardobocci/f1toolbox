from dagster import Definitions, load_assets_from_modules
from assets import fantasy_landing, fantasy_bronze

landing_fantasy_assets = load_assets_from_modules([fantasy_landing])
bronze_fantasy_assets = load_assets_from_modules([fantasy_bronze])

defs = Definitions(
    assets=[*landing_fantasy_assets, *bronze_fantasy_assets],
)