from dagster import MultiPartitionsDefinition, StaticPartitionsDefinition
from src.dagster.assets.constants import EVENTS, SEASONS, YEARS

fantasy_partitions = StaticPartitionsDefinition([*YEARS])

fast_f1_season_partitions = StaticPartitionsDefinition([*SEASONS])
fast_f1_event_partitions = StaticPartitionsDefinition(
    [str(item) for item in max(EVENTS.values())]
)
fast_f1_partitions = MultiPartitionsDefinition(
    {
        "year": fast_f1_season_partitions,
        "event": fast_f1_event_partitions,
    }
)
