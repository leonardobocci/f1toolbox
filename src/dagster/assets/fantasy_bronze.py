import json
import os

import polars as pl
from dagster import MetadataValue, asset
from pyarrow.parquet import read_metadata as parquet_metadata

from src.dagster.assets import constants
from src.dagster.assets.constants import YEARS as years
from src.dagster.utils.fantasy_results_parser import parse_results
from src.dagster.utils.iomanager import polars_to_parquet


def read_landing_fantasy_assets():
    with open(
        f"{constants.landing_FANTASY_PATH}/current_fantasy_assets.json", "r"
    ) as f:
        file = json.load(f)
    last_modified = os.path.getmtime(
        f"{constants.landing_FANTASY_PATH}/current_fantasy_assets.json"
    )
    last_modified_expr = pl.from_epoch(pl.lit(last_modified))
    return file, last_modified_expr


def format_landing_df(asset_type: str, lookup_df: pl.DataFrame) -> pl.DataFrame:
    """Format the constructors and drivers dataframes

    arguments:
        asset_type: str, 'constructors' or 'drivers'
        lookup_df: pl.DataFrame, the lookup dataframe

    returns pl.DataFrame
    """
    file, last_modified_expr = read_landing_fantasy_assets()
    df = pl.LazyFrame(file[asset_type])
    df = df.with_columns((last_modified_expr).alias("last_updated"))
    df1 = df.join(lookup_df, left_on="abbreviation", right_on="id", how="full")
    df1 = df1.select(
        *set(lookup_df.columns) - set(["id", "color"]),
        pl.coalesce("id", "abbreviation").alias("id"),
        pl.coalesce("color", "color_right").alias("color"),
        pl.col("last_updated").fill_null(strategy="min"),
    )
    # this collect should not be required,
    # but without it there is a weird columnnotfound: abbreviation
    # note this occurs also if calling collect(streaming=True)
    return df1.collect()


@asset(
    group_name="bronze_fantasy_files",
    deps=["landing_fantasy_races"],
    compute_kind="polars",
)
def bronze_fantasy_rounds(context):
    """Parse landing zone json to parquet file for fantasy race weekend data"""
    created = False
    for year in years:
        context.log.info(f"Year: {year}")
        with open(f"{constants.landing_FANTASY_PATH}/{year}/races.json", "r") as f:
            file = json.load(f)
        temp_df = pl.LazyFrame(file["races"])
        temp_df = temp_df.with_columns(pl.lit(year).cast(pl.Int64).alias("season"))
        if not created:
            df = temp_df
            created = True
        else:
            df = pl.concat([df, temp_df])
    polars_to_parquet(
        filedir=constants.BRONZE_FANTASY_PATH,
        filename="races",
        data=df,
        context=context,
    )
    meta = parquet_metadata(f"{constants.BRONZE_FANTASY_PATH}/races.parquet").to_dict()
    context.add_output_metadata({"Rows": MetadataValue.int(meta["num_rows"])})
    context.add_output_metadata({"Columns": MetadataValue.int(meta["num_columns"])})
    return


@asset(
    group_name="bronze_fantasy_files",
    deps=["landing_fantasy_constructor_results"],
    compute_kind="polars",
)
def bronze_fantasy_constructor_results(context):
    """Parse landing zone json to parquet file for fantasy constructor results"""
    # Use the generic result parser to parse the driver results
    df = parse_results(context, "constructor")
    # Save them using the iomanager
    polars_to_parquet(
        filedir=constants.BRONZE_FANTASY_PATH,
        filename="constructor_fantasy_attributes",
        data=df,
        context=context,
    )
    meta = parquet_metadata(
        f"{constants.BRONZE_FANTASY_PATH}/constructor_fantasy_attributes.parquet"
    ).to_dict()
    context.add_output_metadata({"Rows": MetadataValue.int(meta["num_rows"])})
    context.add_output_metadata({"Columns": MetadataValue.int(meta["num_columns"])})
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
    return


@asset(
    group_name="bronze_fantasy_files",
    deps=["landing_fantasy_driver_results"],
    compute_kind="polars",
)
def bronze_fantasy_driver_results(context):
    """Parse landing zone json to parquet file for fantasy driver results"""
    # Use the generic result parser to parse the driver results
    df = parse_results(context, "driver")
    # Save them using the iomanager
    polars_to_parquet(
        filedir=constants.BRONZE_FANTASY_PATH,
        filename="driver_fantasy_attributes",
        data=df,
        context=context,
    )
    meta = parquet_metadata(
        f"{constants.BRONZE_FANTASY_PATH}/driver_fantasy_attributes.parquet"
    ).to_dict()
    context.add_output_metadata({"Rows": MetadataValue.int(meta["num_rows"])})
    context.add_output_metadata({"Columns": MetadataValue.int(meta["num_columns"])})
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
    return


@asset(
    group_name="bronze_fantasy_files",
    deps=["landing_fantasy_current_assets", "bronze_fantasy_constructor_results"],
    compute_kind="polars",
)
def bronze_fantasy_current_constructors(context):
    """Parse landing zone json to parquet file for fantasy current constructor info"""
    constructor_lookup = pl.scan_csv(
        "src/dagster/utils/map_fantasy/constructor_mapping.csv"
    )
    unique_constructor_list = (
        pl.scan_parquet(
            f"{constants.BRONZE_FANTASY_PATH}/constructor_fantasy_attributes.parquet"
        )
        .select("id", "color")
        .unique()
    )
    constructor_lookup = unique_constructor_list.join(
        constructor_lookup, on="id", how="full"
    )
    constructor_lookup = constructor_lookup.select(
        pl.coalesce("id", "id_right").alias("id"), "name", "active", "color"
    )
    constructors = format_landing_df("constructors", constructor_lookup)
    polars_to_parquet(
        filedir=constants.BRONZE_FANTASY_PATH,
        filename="constructors",
        data=constructors,
        context=context,
    )
    meta = parquet_metadata(
        f"{constants.BRONZE_FANTASY_PATH}/constructors.parquet"
    ).to_dict()
    context.add_output_metadata({"Rows": MetadataValue.int(meta["num_rows"])})
    context.add_output_metadata({"Columns": MetadataValue.int(meta["num_columns"])})
    return


@asset(
    group_name="bronze_fantasy_files",
    deps=["landing_fantasy_current_assets", "bronze_fantasy_driver_results"],
    compute_kind="polars",
)
def bronze_fantasy_current_drivers(context):
    """Parse landing zone json to parquet file for fantasy current constructor info"""
    drivers_lookup = pl.scan_csv("src/dagster/utils/map_fantasy/driver_mapping.csv")
    unique_driver_list = (
        pl.scan_parquet(
            f"{constants.BRONZE_FANTASY_PATH}/driver_fantasy_attributes.parquet"
        )
        .select("id", "color")
        .unique()
    )
    drivers_lookup = unique_driver_list.join(drivers_lookup, on="id", how="full")
    drivers_lookup = drivers_lookup.select(
        pl.coalesce("id", "id_right").alias("id"), "name", "active", "color"
    )
    drivers = format_landing_df("drivers", drivers_lookup)
    polars_to_parquet(
        filedir=constants.BRONZE_FANTASY_PATH,
        filename="drivers",
        data=drivers,
        context=context,
    )
    meta = parquet_metadata(
        f"{constants.BRONZE_FANTASY_PATH}/drivers.parquet"
    ).to_dict()
    context.add_output_metadata({"Rows": MetadataValue.int(meta["num_rows"])})
    context.add_output_metadata({"Columns": MetadataValue.int(meta["num_columns"])})
    return
