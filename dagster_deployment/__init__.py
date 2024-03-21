from dagster import Definitions, load_assets_from_modules, AutoMaterializePolicy, AutoMaterializeRule
from assets import fantasy_landing, fantasy_bronze

landing_fantasy_assets = load_assets_from_modules([fantasy_landing])
#Downstream layers are auto materialized whenever the upstream layer is materialized
wait_for_all_parents_policy = AutoMaterializePolicy.eager().with_rules(
    AutoMaterializeRule.skip_on_not_all_parents_updated()
)
bronze_fantasy_assets = load_assets_from_modules([fantasy_bronze],auto_materialize_policy=wait_for_all_parents_policy)

defs = Definitions(
    assets=[*landing_fantasy_assets, *bronze_fantasy_assets],
)