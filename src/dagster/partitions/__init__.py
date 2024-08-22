from dagster import MultiPartitionsDefinition, StaticPartitionsDefinition
from src.dagster.assets.constants import EVENTS, SEASONS, YEARS  # , SESSIONS

fantasy_partitions = StaticPartitionsDefinition([*YEARS])

fast_f1_season_partitions = StaticPartitionsDefinition([*SEASONS])

fast_f1_multi_partitions = MultiPartitionsDefinition(
    {
        "season": StaticPartitionsDefinition([*SEASONS]),
        # TODO: Figure out how to make event partition dynamic based on year
        "event": StaticPartitionsDefinition([*EVENTS]),
    }
)
