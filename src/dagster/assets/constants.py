import os
from pathlib import Path

from dagster_dbt import DbtCliResource

dbt_project_dir = Path(__file__).joinpath("..", "..", "..", "dbt").resolve()
dbt = DbtCliResource(project_dir=os.fspath(dbt_project_dir))

# If DAGSTER_DBT_PARSE_PROJECT_ON_LOAD is set, a manifest will be created at run time.
# Otherwise, we expect a manifest to be present in the project's target directory.
if os.getenv("DAGSTER_DBT_PARSE_PROJECT_ON_LOAD"):
    dbt_manifest_path = (
        dbt.cli(
            ["--quiet", "parse"],
            target_path=Path("target"),
        )
        .wait()
        .target_path.joinpath("manifest.json")
    )
else:
    dbt_manifest_path = dbt_project_dir.joinpath("target", "manifest.json")

# [Fantasy]
YEARS = ["2022", "2023", "2024"]

landing_FANTASY_PATH = "data/landing/fantasy"
BRONZE_FANTASY_PATH = "data/bronze/fantasy"

# [FastF1]
SEASONS = ["2022", "2023", "2024"]
EVENTS_22 = [str(i) for i in range(1, 23)]  # 22 races
EVENTS_23 = [str(i) for i in range(1, 23)]  # 22 races
EVENTS_24 = [str(i) for i in range(1, 25)]  # 24 races
EVENTS = EVENTS_24
SESSIONS = ["1", "2", "3", "4", "5"]

landing_FASTF1_PATH = "data/landing/fastf1"
BRONZE_FASTF1_PATH = "data/bronze/fastf1"
