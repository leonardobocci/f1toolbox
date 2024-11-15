from google.cloud import bigquery


def load_parquet_file_to_table(file_uri: str, table_id: str) -> dict:
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    load_job = client.load_table_from_uri(file_uri, table_id, job_config=job_config)

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    return {
        "num_rows": destination_table.num_rows,
        "num_cols": len(destination_table.schema),
    }
