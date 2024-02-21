from dagster import Definitions, load_assets_from_modules, AutoMaterializePolicy
from assets import fantasy_landing, fantasy_bronze

landing_fantasy_assets = load_assets_from_modules([fantasy_landing])
#Downstream layers are auto materialized whenever the upstream layer is materialized
bronze_fantasy_assets = load_assets_from_modules([fantasy_bronze], auto_materialize_policy=AutoMaterializePolicy.eager())

defs = Definitions(
    assets=[*landing_fantasy_assets, *bronze_fantasy_assets],
)