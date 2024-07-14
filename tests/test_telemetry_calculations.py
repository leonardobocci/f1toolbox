import sys # this allows running with pytest command both from parent and current dir
sys.path.append("../")
sys.path.append("./")

from src.dagster.utils.fastf1_parser import enrich_fastf1_telemetry
from unittest.mock import MagicMock
import polars as pl
from datetime import timedelta
import datetime
import zoneinfo
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
df = df.with_columns([
    pl.col("time").diff().alias("delta_time"),
    pl.col("Speed").diff().alias("delta_speed"),
    ])

data_exp = [{'Speed': 100,
  'time': datetime.datetime(2023, 3, 3, 11, 15, 4, 569050, tzinfo=zoneinfo.ZoneInfo(key='UTC')),
  'X': 0,
  'Y': 0,
  'delta_time': None,
  'delta_speed': None,
  'lateral_acceleration': 13.802888749998703,
  'longitudinal_acceleration': None},
 {'Speed': 150,
  'time': datetime.datetime(2023, 3, 3, 11, 15, 4, 569240, tzinfo=zoneinfo.ZoneInfo(key='UTC')),
  'X': 5,
  'Y': 15,
  'delta_time': datetime.timedelta(microseconds=190),
  'delta_speed': 50,
  'lateral_acceleration': 21.739549781247963,
  'longitudinal_acceleration': 73099.41520467837},
 {'Speed': 50,
  'time': datetime.datetime(2023, 3, 3, 11, 15, 4, 569400, tzinfo=zoneinfo.ZoneInfo(key='UTC')),
  'X': 0,
  'Y': 5,
  'delta_time': datetime.timedelta(microseconds=160),
  'delta_speed': -100,
  'lateral_acceleration': 3.4160406822806566,
  'longitudinal_acceleration': -173611.11111111112}]
expected_df = pl.LazyFrame(data_exp)

def test_enrich_fastf1_telemetry():
    got_df = enrich_fastf1_telemetry(context, df)
    assert got_df.collect().equals(expected_df.collect())