import json

import gcsfs
import polars as pl
from google.cloud import storage

from dagster import InputContext, IOManager, MetadataValue, OutputContext


def dagster_asset_path_identifier(
    optional_prefix: str, context: InputContext | OutputContext
) -> str:
    "Used to retrieve partition keys for partitioned assets"
    if context.has_asset_partitions and context.has_partition_key:
        return "/".join(
            [optional_prefix, *context.asset_key.path, context.partition_key]
        )
    else:
        return "/".join([optional_prefix, *context.asset_key.path])


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
            context.log.debug("Found partitions in inputcontext")
            if context.has_partition_key:
                context.log.debug(
                    "Current run has partition key. Loading single partition."
                )
                # load a single partition
                blob = bucket.blob(
                    f"{dagster_asset_path_identifier(self.prefix, context)}.json"
                )
                data = blob.download_as_string()
                return json.loads(data)
            else:
                context.log.debug(
                    "Current run is not partitioned. Loading all partitions."
                )
                # load all partitions
                partitions = context.asset_partitions_def.get_partition_keys()
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
                "Did not find inputcontext partitions. Trying to load non-partitioned json asset..."
            )
            blob = bucket.blob(
                f"{dagster_asset_path_identifier(self.prefix, context.upstream_output)}.json"
            )
            data = blob.download_as_string()
            return json.loads(data)


class GCSPolarsParquetIOManager(IOManager):
    """Read and write parquet files to GCS. Writes a single parquet file for each partition and returns a list of lazyframes for each loaded partition, or a lazyframe for non-partitioned assets."""

    def __init__(self, project: str, bucket_name: str, optional_prefix: str = ""):
        self.project = project
        self.bucket_name = bucket_name
        self.prefix = optional_prefix
        self.fs = gcsfs.GCSFileSystem(project=project)
        self.client = storage.Client(project=project)

    def handle_output(self, context: OutputContext, data: pl.LazyFrame | pl.DataFrame):
        with self.fs.open(
            f"{self.bucket_name}/{dagster_asset_path_identifier(self.prefix, context)}.parquet",
            "wb",
        ) as f:
            if isinstance(data, pl.DataFrame):
                data.write_parquet(f)
                num_rows = data.select(pl.len()).item()
                num_cols = data.width
            elif isinstance(data, pl.LazyFrame):
                # not supported for all operations so cannot always use (eg. sink to cloud storage not supported, any multi-row operation not supported)
                try:
                    data.sink_parquet(f)
                except Exception as e:
                    context.log.debug(
                        f"Could not write parquet file using sink_parquet. Trying to collect and write. {e}"
                    )
                    try:
                        data.collect(streaming=True).write_parquet(f)
                    except Exception as e:
                        context.log.debug(
                            f"Could not write parquet file using collect in streaming mode. Trying to collect and write without streaming. {e}"
                        )
                        data.collect().write_parquet(f)
                num_rows = data.select(pl.len()).collect().item()
                num_cols = data.collect_schema().len()
            elif data is None:
                context.log.warning(
                    "Iomanager received no data to write. Writing empty file to prevent downstream asset failures..."
                )
                pl.DataFrame().write_parquet(f)
                num_cols = 0
                num_rows = 0
            else:
                raise NotImplementedError(
                    f"Data type not supported. Received: {type(data)}. Supported types: pl.LazyFrame, pl.DataFrame"
                )
            context.add_output_metadata({"Rows": MetadataValue.int(num_rows)})
            context.add_output_metadata({"Columns": MetadataValue.int(num_cols)})

    def load_input(self, context: InputContext) -> pl.LazyFrame:
        if context.has_asset_partitions:
            context.log.debug("Found partitions in inputcontext")
            if context.has_partition_key:
                context.log.debug(
                    "Current run has partition key. Loading single partition."
                )
                # load a single partition
                with self.fs.open(
                    f"{self.bucket_name}/{dagster_asset_path_identifier(self.prefix, context)}.parquet",
                    "rb",
                ) as f:
                    return pl.scan_parquet(f)
            else:
                context.log.debug(
                    "Current run is not partitioned. Loading all partitions."
                )
                # load all partitions
                partitions = context.asset_partitions_def.get_partition_keys()
                dfs = [
                    pl.scan_parquet(
                        self.fs.open(
                            f"{self.bucket_name}/{'/'.join([self.prefix, *context.upstream_output.asset_key.path, partition])}.parquet",
                            "rb",
                        )
                    )
                    for partition in partitions
                ]
                return [
                    df for df in dfs if df.collect_schema()
                ]  # filter out empty partitions
        else:
            context.log.debug(
                "Did not find inputcontext partitions. Trying to load non-partitioned parquet asset..."
            )
            with self.fs.open(
                f"{self.bucket_name}/{dagster_asset_path_identifier(self.prefix, context.upstream_output)}.parquet",
                "rb",
            ) as f:
                return pl.scan_parquet(f)
