Local deployment:

Clickhouse:
./clickhouse server
./clickhouse client
Note on creation of users: https://www.markhneedham.com/blog/2023/11/07/clickhouse-no-writeable-access-storage/

Poetry:
sudo apt update
sudo apt install pipx
pipx ensurepath
sudo pipx ensurepath --global # optional to allow pipx actions with --global argument
pipx install poetry
poetry install
poetry shell (to activate venv)

Dagster (env vars):
DAGSTER_DBT_PARSE_PROJECT_ON_LOAD=1
DAGSTER_HOME="/home/leo/f1/src/dagster/localhome"

DBT (in dbt directory):
dbt deps (to install column level lineage dagster dependency)