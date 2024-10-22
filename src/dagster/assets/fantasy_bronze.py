import polars as pl

from dagster import asset
from src.dagster.utils.fantasy_results_parser import parse_results


def format_landing_df(df: pl.LazyFrame, lookup_df: pl.LazyFrame) -> pl.DataFrame:
    """Format the constructors and drivers dataframes

    arguments:
        asset_type: str, 'constructors' or 'drivers'
        lookup_df: pl.DataFrame, the lookup dataframe

    returns pl.DataFrame
    """
    joined = df.join(lookup_df, left_on="abbreviation", right_on="id", how="full")
    joined = joined.select(
        *set(lookup_df.columns) - set(["id"]),
        pl.coalesce("id", "abbreviation").alias("id"),
    )
    # this collect should not be required,
    # but without it there is a notYetImplemented polars exception
    # TODO: open an issue in polars github. Last tested on polars 1.10.0
    return joined.collect()


@asset(
    group_name="bronze_fantasy_files",
    compute_kind="polars",
    io_manager_key="gcs_parquet_fantasy_bronze_io_manager",
)
def bronze_fantasy_rounds(context, landing_fantasy_races):
    """Parse landing zone json to parquet file for fantasy race weekend data"""
    partition_keys = context.asset_partition_keys_for_input("landing_fantasy_races")
    # Add the partition key as a literal column in the DataFrame
    dfs = [
        pl.LazyFrame(data["races"]).with_columns(
            pl.lit(partition_key).cast(pl.Int64).alias("season")
        )
        for data, partition_key in zip(landing_fantasy_races, partition_keys)
    ]
    df = pl.concat(dfs)
    return df


@asset(
    group_name="bronze_fantasy_files",
    compute_kind="polars",
    io_manager_key="gcs_parquet_fantasy_bronze_io_manager",
)
def bronze_fantasy_constructor_results(context, landing_fantasy_constructor_results):
    """Parse landing zone json to parquet file for fantasy constructor results"""
    partition_keys = context.asset_partition_keys_for_input(
        "landing_fantasy_constructor_results"
    )
    # Use the generic result parser to parse the driver results
    dfs = [
        parse_results(context, data, "constructor", partition_key)
        for data, partition_key in zip(
            landing_fantasy_constructor_results, partition_keys
        )
    ]
    df = pl.concat(dfs, how="diagonal")
    """Received schema:
    [{}] response: list of 10 dictionaries (one per constructor) [ {}, {} ]
        abbreviation: str,
        color: str,
        constructor : bool,
        [{}] race_results: list of 2 to 4 dicts (one dict per result type) [ {}, {} ]
            [0] [{}, {}]
                "fantasy_results":
                    list [ 17 dicts: {'id': 'attr1', 'points_per_race_list': [X0, X1, ... Xlastrace] }, {}],
                "id": "weekend_current_points",
                "results_per_aggregation_list": [],
                "results_per_race_list": [X0, X1, ... Xlastrace]
            [1] (2023+) {} price_at_lock dict:
                dict {'id': 'price_at_lock', 'results_per_aggregation_list': [], 'results_per_race_list': [X0, X1, ... Xlastrace]},
            [2 or 1(2022)] {} price_change:
                dict {'id': 'price_change', 'results_per_aggregation_list': [], 'results_per_race_list': [X0, X1, ... Xlastrace]},
            [3] (2023+) weekend_PPM:
                dict {'id': 'weekend_PPM', 'results_per_aggregation_list': [], 'results_per_race_list': [X0, X1, ... Xlastrace]}
    """
    return df


@asset(
    group_name="bronze_fantasy_files",
    compute_kind="polars",
    io_manager_key="gcs_parquet_fantasy_bronze_io_manager",
)
def bronze_fantasy_driver_results(context, landing_fantasy_driver_results):
    partition_keys = context.asset_partition_keys_for_input(
        "landing_fantasy_driver_results"
    )
    # Use the generic result parser to parse the driver results
    dfs = [
        parse_results(context, data, "driver", partition_key)
        for data, partition_key in zip(landing_fantasy_driver_results, partition_keys)
    ]
    df = pl.concat(dfs, how="diagonal")
    """Received schema:
    [{}] response: list of 20 dictionaries (one per driver) [ {}, {} ]
        abbreviation: str,
        color: str,
        constructor : bool,
        [{}] race_results: list of 16 dicts (one dict per result type) [ {}, {} ]
            [0] [{}, {}]
                "fantasy_results":
                    list [ 16 dicts: {'id': 'attr1', 'points_per_race_list': [X0, X1, ... Xlastrace] }, {}],
                "id": "weekend_current_points",
                "results_per_aggregation_list": [],
                "results_per_race_list": [X0, X1, ... Xlastrace]
            [1] (2023+) {} price_at_lock dict:
                dict {'id': 'price_at_lock', 'results_per_aggregation_list': [], 'results_per_race_list': [X0, X1, ... Xlastrace]},
            [2] (2023+)] {} price_change:
                dict {'id': 'price_change', 'results_per_aggregation_list': [], 'results_per_race_list': [X0, X1, ... Xlastrace]},
            [3] (2023+) weekend_PPM:
                dict {'id': 'weekend_PPM', 'results_per_aggregation_list': [], 'results_per_race_list': [X0, X1, ... Xlastrace]}
            [4-15] Not Required - Actual race results, not fantasy-related
    """
    return df


@asset(
    group_name="bronze_fantasy_files",
    compute_kind="polars",
    io_manager_key="gcs_parquet_fantasy_bronze_io_manager",
)
def bronze_fantasy_current_constructors(
    context, bronze_fantasy_constructor_results, landing_fantasy_current_assets
):
    """Parse landing zone json to parquet file for fantasy current constructor info"""
    constructor_lookup = pl.scan_csv(
        "src/dagster/utils/map_fantasy/constructor_mapping.csv"
    )
    unique_constructor_list = bronze_fantasy_constructor_results.select("id").unique()
    constructor_lookup = unique_constructor_list.join(
        constructor_lookup, on="id", how="full"
    )
    constructor_lookup = constructor_lookup.select(
        pl.coalesce("id", "id_right").alias("id"), "name", "active"
    )
    landing_fantasy_current_assets_df = pl.LazyFrame(
        landing_fantasy_current_assets["constructors"]
    )
    df = format_landing_df(landing_fantasy_current_assets_df, constructor_lookup)
    return df


@asset(
    group_name="bronze_fantasy_files",
    compute_kind="polars",
    io_manager_key="gcs_parquet_fantasy_bronze_io_manager",
)
def bronze_fantasy_current_drivers(
    context, bronze_fantasy_driver_results, landing_fantasy_current_assets
):
    """Parse landing zone json to parquet file for fantasy current constructor info"""
    drivers_lookup = pl.scan_csv("src/dagster/utils/map_fantasy/driver_mapping.csv")
    unique_driver_list = bronze_fantasy_driver_results.select("id").unique()
    drivers_lookup = unique_driver_list.join(drivers_lookup, on="id", how="full")
    drivers_lookup = drivers_lookup.select(
        pl.coalesce("id", "id_right").alias("id"), "name", "active"
    )
    landing_fantasy_current_assets_df = pl.LazyFrame(
        landing_fantasy_current_assets["drivers"]
    )
    df = format_landing_df(landing_fantasy_current_assets_df, drivers_lookup)
    return df
