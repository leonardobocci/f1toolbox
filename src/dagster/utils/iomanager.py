import json
import os
from typing import Union

import gcsfs
import polars as pl
from google.cloud import storage

from dagster import InputContext, IOManager, OutputContext
from src.dagster.assets import constants


def dagster_output_identifier(
    context: InputContext | OutputContext, optional_prefix: str
) -> str:
    "Used to retrieve partition keys for partitioned assets"
    if context.has_partition_key:
        return "/".join(
            [optional_prefix, *context.asset_key.path, context.asset_partition_key]
        )
    else:
        return "/".join([optional_prefix, *context.asset_key.path])


class GcsJsonIoManager(IOManager):
    """Read and write json files to GCS"""

    def __init__(self, project, bucket_name, optional_prefix: str = ""):
        self.project = project
        self.bucket_name = bucket_name
        self.prefix = optional_prefix
        self.client = storage.Client(project=project)

    def handle_output(self, context: OutputContext, obj):
        bucket = self.client.bucket(self.bucket_name)
        blob = bucket.blob(f"{dagster_output_identifier(context, self.prefix)}.json")
        blob.upload_from_string(json.dumps(obj))

    def load_input(self, context: InputContext):
        """Load each of the json partitions."""
        bucket = self.client.bucket(self.bucket_name)
        partitions = context.asset_partitions_def.get_partition_keys()
        blobs = [
            bucket.blob(
                f"{"/".join([self.prefix, *context.upstream_output.asset_key.path, partition])}.json"
            )
            for partition in partitions
        ]
        data = [blob.download_as_string() for blob in blobs]
        return [json.loads(d) for d in data]


class GCSPolarsParquetIOManager(IOManager):
    """Read and write parquet files to GCS"""

    def __init__(self, project: str, bucket_name: str, optional_prefix: str = ""):
        self.project = project
        self.bucket_name = bucket_name
        self.prefix = optional_prefix
        self.fs = gcsfs.GCSFileSystem(project=project)

    def handle_output(self, context: OutputContext, data: pl.LazyFrame | pl.DataFrame):
        with self.fs.open(
            f"{self.bucket_name}/{dagster_output_identifier(context, self.prefix)}.parquet",
            "wb",
        ) as f:
            if isinstance(data, pl.DataFrame):
                data.write_parquet(f)
            elif isinstance(data, pl.LazyFrame):
                # not supported for all operations so cannot always use (eg. sink to cloud storage not supported, any multi-row operation not supported)
                try:
                    data.sink_parquet(f)
                except Exception as e:
                    context.log.debug(
                        f"Could not write parquet file using sink_parquet. Trying to collect and write. {e}"
                    )
                    data.collect(streaming=True).write_parquet(f)
            else:
                raise NotImplementedError("Data type not supported")

    def load_input(self, context: InputContext) -> pl.LazyFrame:
        with self.fs.open(
            f"{self.bucket_name}/{dagster_output_identifier(context.upstream_output, self.prefix)}.parquet",
            "rb",
        ) as f:
            return pl.scan_parquet(f)


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


def save_landing_fantasy_json(data: dict, filename: str, year: int = None) -> None:
    """Save the raw data extracted from the Fantasy APIs as json files"""
    if year:
        landing_fantasy_dir = f"{constants.landing_FANTASY_PATH}/{year}"
    else:
        landing_fantasy_dir = f"{constants.landing_FANTASY_PATH}/"
    _save_generic_json(data, filename, landing_fantasy_dir)
    return


def save_landing_fastf1_json(
    data: dict, filename: str, year: int = None, subdirectory: str = None
) -> None:
    """Save the raw data extracted from the FastF1 APIs as json files"""
    if year:
        landing_fastf1_dir = f"{constants.landing_FASTF1_PATH}/{year}"
    else:
        landing_fastf1_dir = f"{constants.landing_FASTF1_PATH}/"
    if subdirectory:
        landing_fastf1_dir = f"{landing_fastf1_dir}/{subdirectory}"
    _save_generic_json(data, filename, landing_fastf1_dir)
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
            context.log.debug(
                f"Could not write parquet file using sink_parquet. Trying to collect and write. {e}"
            )
            data.collect(streaming=True).write_parquet(f"{filedir}/{filename}.parquet")
    else:
        raise NotImplementedError("Data type not supported")
    return


def parquet_to_polars(filedir: str, context) -> pl.LazyFrame:
    """Read a parquet file to polars frame"""
    return pl.scan_parquet(filedir)
