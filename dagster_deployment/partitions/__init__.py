from dagster import StaticPartitionsDefinition, MultiPartitionsDefinition

from assets.constants import YEARS, SEGMENTS, SEASONS

fantasy_partitions = StaticPartitionsDefinition(
    [*YEARS]
)

fast_f1_season_partitions = StaticPartitionsDefinition(
    [*SEASONS]
)

fast_f1_multi_partitions = MultiPartitionsDefinition(
    {
        "season": StaticPartitionsDefinition([*SEASONS]),
        "segment": StaticPartitionsDefinition([*SEGMENTS])
    }
)