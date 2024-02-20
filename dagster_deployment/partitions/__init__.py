from dagster import StaticPartitionsDefinition

from assets.constants import YEARS

fantasy_partitions = StaticPartitionsDefinition(
    [*YEARS]
)