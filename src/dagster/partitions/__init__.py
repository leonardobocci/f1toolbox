from dagster import StaticPartitionsDefinition
from src.dagster.assets.constants import SEASONS, YEARS

fantasy_partitions = StaticPartitionsDefinition([*YEARS])

fast_f1_season_partitions = StaticPartitionsDefinition([*SEASONS])
