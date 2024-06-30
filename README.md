Local deployment:

Clickhouse:
./clickhouse server
./clickhouse client
Note on creation of users: https://www.markhneedham.com/blog/2023/11/07/clickhouse-no-writeable-access-storage/

Dagster (env vars):
DAGSTER_DBT_PARSE_PROJECT_ON_LOAD=1
DAGSTER_HOME="/home/leo/f1/src/dagster/localhome"