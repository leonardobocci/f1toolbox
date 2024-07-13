import sys # this allows running with pytest command both from parent and current dir
sys.path.append("../")
sys.path.append("./")

from src.dagster.utils import enrich_fastf1_telemetry
from unittest.mock import MagicMock
import polars as pl
from datetime import timedelta, datetime
import dateutil.parser

context = MagicMock()

datetime_obj = dateutil.parser.isoparse('2023-03-03T11:15:04.569Z')

data = {
    "Speed": [100, 150, 50],
    "time": [datetime_obj + timedelta(microseconds=50), datetime_obj + timedelta(microseconds=240), datetime_obj + timedelta(microseconds=100)],
    "X": [0, 5, 0],
    "Y": [0, 15, 5],
    "x_prev_1": [-5, 0, 5],
    "y_prev_1": [-10, 0, 15],
    "x_prev_2": [-10, -5, 0],
    "y_prev_2": [-20, -10, 0],
}
df = pl.LazyFrame(data)
df = df.with_columns(pl.col("time").diff().alias("delta_time"))
expected_df = pl.LazyFrame()

def test_enrich_fastf1_telemetry():
    got_df = enrich_fastf1_telemetry(context, df)
    print(got_df.collect())