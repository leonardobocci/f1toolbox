from dagster import Definitions, load_assets_from_modules, AutoMaterializePolicy, AutoMaterializeRule
from assets import fantasy_landing, fantasy_bronze, fastf1_landing, fastf1_bronze

#Downstream layers are auto materialized whenever the upstream layer is materialized
wait_for_all_parents_policy = AutoMaterializePolicy.eager().with_rules(
    AutoMaterializeRule.skip_on_not_all_parents_updated()
)

landing_fantasy_assets = load_assets_from_modules([fantasy_landing])
bronze_fantasy_assets = load_assets_from_modules([fantasy_bronze],auto_materialize_policy=wait_for_all_parents_policy)

landing_fastf1_assets = load_assets_from_modules([fastf1_landing])
bronze_fastf1_assets = load_assets_from_modules([fastf1_bronze],auto_materialize_policy=wait_for_all_parents_policy)

defs = Definitions(
    assets=[*landing_fantasy_assets, *bronze_fantasy_assets, *landing_fastf1_assets, *bronze_fastf1_assets],
)