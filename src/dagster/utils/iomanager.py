import json
import os

import gcsfs
import polars as pl
import pyarrow as pa
from google.cloud import storage
from pyarrow.parquet import read_metadata as parquet_metadata

from dagster import (
    InputContext,
    IOManager,
    MetadataValue,
    MultiPartitionKey,
    OutputContext,
)


def dagster_asset_path_identifier(
    optional_prefix: str, context: InputContext | OutputContext
) -> str:
    "Used to retrieve partition keys for partitioned assets"
    if context.has_asset_partitions and context.has_partition_key:
        if isinstance(context.partition_key, MultiPartitionKey):
            partition_key = "_".join(context.partition_key.keys_by_dimension.values())
        else:
            partition_key = context.partition_key
        return "/".join([optional_prefix, *context.asset_key.path, partition_key])
    else:
        return "/".join([optional_prefix, *context.asset_key.path])


def get_partitioned_input_list(context: InputContext) -> list[str]:
    partition_list = []
    partitions = context.asset_partitions_def.get_partition_keys()
    for partition in partitions:
        if isinstance(partition, MultiPartitionKey):
            partition_list.append("_".join(partition.keys_by_dimension.values()))
        else:
            partition_list.append(partition)
    return partition_list


class GcsJsonIoManager(IOManager):
    """Read and write json files to GCS. Writes a single json file for each partition and returns a list of dicts for each loaded partition, or a dict for non-partitioned assets."""

    def __init__(self, project, bucket_name, optional_prefix: str = ""):
        self.project = project
        self.bucket_name = bucket_name
        self.prefix = optional_prefix
        self.client = storage.Client(project=project)

    def handle_output(self, context: OutputContext, obj):
        if obj is None:
            context.log.warning(
                "Iomanager received no data to write. Writing empty file to prevent downstream asset failures..."
            )
            obj = []
        bucket = self.client.bucket(self.bucket_name)
        blob = bucket.blob(
            f"{dagster_asset_path_identifier(self.prefix, context)}.json"
        )
        blob.upload_from_string(json.dumps(obj))
        context.add_output_metadata({"Length": MetadataValue.int(len(obj))})

    def load_input(self, context: InputContext):
        """Load each of the json partitions."""
        bucket = self.client.bucket(self.bucket_name)
        if context.has_asset_partitions:
            context.log.debug("Found partitions in inputcontext (upstream asset)")
            if context.has_partition_key:
                context.log.debug(
                    "Current run is partitioned. Loading single partition."
                )
                # load a single partition
                blob = bucket.blob(
                    f"{dagster_asset_path_identifier(self.prefix, context)}.json"
                )
                data = blob.download_as_string()
                return json.loads(data)
            else:
                context.log.debug(
                    "Current run is not partitioned but upstream output is. Loading all non-empty partitions."
                )
                # load all partitions
                partitions = get_partitioned_input_list(context)
                blobs = [
                    bucket.blob(
                        f"{'/'.join([self.prefix, *context.upstream_output.asset_key.path, partition])}.json"
                    )
                    for partition in partitions
                ]
                data = [blob.download_as_string() for blob in blobs]
                loaded_dicts = [json.loads(d) for d in data]
                return [
                    item for item in loaded_dicts if item
                ]  # filter out empty partitions
        else:
            context.log.debug(
                "Upstream asset is not partitioned. Loading single json asset..."
            )
            blob = bucket.blob(
                f"{dagster_asset_path_identifier(self.prefix, context)}.json"
            )
            data = blob.download_as_string()
            return json.loads(data)


class GCSPolarsParquetIOManager(IOManager):
    """Read and write parquet files to GCS.
    Writes a single parquet file for each partition and returns
    a list of lazyframe paths for each loaded partition,
    or a single lazyframe path for non-partitioned assets.
    This is not loading data directly, as polars does not support scanning from memory buffers."""

    def __init__(self, project: str, bucket_name: str, optional_prefix: str = ""):
        self.project = project
        self.bucket_name = bucket_name
        self.prefix = optional_prefix
        self.fs = gcsfs.GCSFileSystem(project=project)
        self.client = storage.Client(project=project)

    def handle_output(self, context: OutputContext, data: pl.LazyFrame | pl.DataFrame):
        if data is None:
            context.log.warning(
                "Iomanager received no data to write. Writing empty file to prevent downstream asset failures..."
            )
            data = pl.DataFrame()
        if isinstance(data, pl.DataFrame):
            context.log.debug("Received polars DataFrame. Writing to parquet...")
            with self.fs.open(
                f"{self.bucket_name}/{dagster_asset_path_identifier(self.prefix, context)}.parquet",
                "wb",
            ) as f:
                data.write_parquet(f)
                num_rows = data.select(pl.len()).item()
                num_cols = data.width
        elif isinstance(data, pl.LazyFrame):
            context.log.debug(
                "Received polars LazyFrame. Sinking to tmp parquet then uploading in chunks..."
            )
            filedir = f"{dagster_asset_path_identifier(self.prefix, context)}"
            os.makedirs(f"/tmp/{filedir}", exist_ok=True)
            filepath = f"/tmp/{filedir}.parquet"
            context.log.debug(f"Writing to: {filepath}")
            try:
                data.sink_parquet(filepath)
                bucket = self.client.bucket(self.bucket_name)
                blob = bucket.blob(f"{filedir}.parquet", chunk_size=1048576)  # 1MiB
                with open(filepath, "rb") as file_obj:
                    blob.upload_from_file(file_obj)
                context.log.debug(
                    "Uploaded parquet file to GCS. Cleaning up tmp local file..."
                )
                meta = parquet_metadata(filepath).to_dict()
                os.remove(filepath)
                os.rmdir(f"/tmp/{filedir}")
                num_rows = meta["num_rows"]
                num_cols = meta["num_columns"]
            except (
                pl.exceptions.ComputeError,
                pl.exceptions.InvalidOperationError,
            ) as e:
                if "not yet implemented" in str(e) or "not yet supported" in str(e):
                    context.log.warning(
                        f"Error sinking parquet: {e}. Trying to collect and write"
                    )
                    data = data.collect()
                    with self.fs.open(
                        f"{self.bucket_name}/{filedir}.parquet",
                        "wb",
                    ) as f:
                        data.write_parquet(f)
                        num_rows = data.select(pl.len()).item()
                        num_cols = data.width
                else:
                    raise e
        else:
            raise NotImplementedError(
                f"Data type not supported. Received: {type(data)}. Supported types: pl.LazyFrame, pl.DataFrame"
            )
        context.add_output_metadata({"Rows": MetadataValue.int(num_rows)})
        context.add_output_metadata({"Columns": MetadataValue.int(num_cols)})

    def load_input(self, context: InputContext) -> str | list[str]:
        if context.has_asset_partitions:
            context.log.debug("Found partitions in inputcontext (upstream asset)")
            if context.has_partition_key:
                context.log.debug(
                    "Current run is partitioned. Returning single partition path."
                )
                return f"gs://{self.bucket_name}/{dagster_asset_path_identifier(self.prefix, context)}.parquet"
            else:
                context.log.debug(
                    "Current run is not partitioned but upstream output is. Returning all non-empty partition paths."
                )
                partitions = get_partitioned_input_list(context)
                filepaths = []
                for partition in partitions:
                    filepath = f"gs://{self.bucket_name}/{'/'.join([self.prefix, *context.upstream_output.asset_key.path, partition])}.parquet"
                    try:
                        parquet_metadata(filepath)
                        filepaths.append(filepath)
                    except pa.lib.ArrowInvalid:
                        context.log.warning(
                            f"Found empty partition: {partition} in file {filepath} Skipping..."
                        )
                return filepaths
        else:
            context.log.debug(
                "Upstream asset is not partitioned. Returning non-partitioned file path..."
            )
            return f"gs://{self.bucket_name}/{dagster_asset_path_identifier(self.prefix, context)}.parquet"
