from dagster import StaticPartitionsDefinition

from dagster_deployment.assets.constants import YEARS

fantasy_partitions = StaticPartitionsDefinition(
    [*YEARS]
)