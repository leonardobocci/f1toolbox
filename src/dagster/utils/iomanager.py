import json
import os
from typing import Union

import polars as pl
from assets import constants


def _save_generic_json(data: dict, filename: str, filedir: str) -> None:
    """Save the raw data extracted from the APIs as json files"""
    if data:
        filepath = os.path.join(filedir, f"{filename}.json")
        os.makedirs(filedir, exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    else:
        context.log.warning(f"Empty dict received for {filename}")  # noqa: F821
    return


def save_raw_fantasy_json(data: dict, filename: str, year: int = None) -> None:
    """Save the raw data extracted from the Fantasy APIs as json files"""
    if year:
        raw_fantasy_dir = f"{constants.RAW_FANTASY_PATH}/{year}"
    else:
        raw_fantasy_dir = f"{constants.RAW_FANTASY_PATH}/"
    _save_generic_json(data, filename, raw_fantasy_dir)
    return


def save_raw_fastf1_json(
    data: dict, filename: str, year: int = None, subdirectory: str = None
) -> None:
    """Save the raw data extracted from the FastF1 APIs as json files"""
    if year:
        raw_fastf1_dir = f"{constants.RAW_FASTF1_PATH}/{year}"
    else:
        raw_fastf1_dir = f"{constants.RAW_FASTF1_PATH}/"
    if subdirectory:
        raw_fastf1_dir = f"{raw_fastf1_dir}/{subdirectory}"
    _save_generic_json(data, filename, raw_fastf1_dir)
    return


def polars_to_parquet(
    filedir: str, filename: str, data: Union[pl.DataFrame, pl.LazyFrame], context
) -> None:
    """Write a polars frame to parquet file"""

    os.makedirs(filedir, exist_ok=True)
    if isinstance(data, pl.DataFrame):
        data.write_parquet(f"{filedir}/{filename}.parquet")
    elif isinstance(data, pl.LazyFrame):
        # not supported for all operations so cannot always use
        try:
            data.sink_parquet(f"{filedir}/{filename}.parquet")
        except Exception as e:
            print(e)
            context.log.warning(
                f"Could not write parquet file using sink_parquet. Trying to collect and write. {e}"
            )
            data.collect(streaming=True).write_parquet(f"{filedir}/{filename}.parquet")
    else:
        raise NotImplementedError("Data type not supported")
    return
