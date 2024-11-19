import sys  # this allows running with pytest command both from parent and current dir

sys.path.append("../")
sys.path.append("./")

import datetime
from datetime import timedelta
from unittest.mock import MagicMock

import dateutil.parser
import polars as pl

from src.dagster.utils.fastf1_parser import enrich_fastf1_telemetry

context = MagicMock()

datetime_obj = dateutil.parser.isoparse("2024-01-30T12:15:05.500Z")

data = {
    "Speed": [100, 150, 50],
    "time": [
        datetime_obj + timedelta(microseconds=50),
        datetime_obj + timedelta(microseconds=240),
        datetime_obj + timedelta(microseconds=340),
    ],
    "X": [0, 5, 0],
    "Y": [0, 15, 5],
    "x_prev_1": [-5, 0, 5],
    "y_prev_1": [-10, 0, 15],
    "x_prev_2": [-10, -5, 0],
    "y_prev_2": [-20, -10, 0],
}
df = pl.LazyFrame(data)
df = df.with_columns(
    [
        pl.col("time").diff().alias("delta_time"),
        pl.col("Speed").diff().alias("delta_speed"),
    ]
)

# https://planetcalc.com/8116/ To get circle centers and radiuses for testing
# remember to set precision to 20 digits
# see the excel file (open document format) accel_calculations

data_exp = [
    {
        "Speed": 100,
        "time": datetime_obj + timedelta(microseconds=50),
        "X": 0,
        "Y": 0,
        "delta_time": None,
        "delta_speed": None,
        "lateral_acceleration": None,  # due to collinear points
        "longitudinal_acceleration": None,  # due to lack of prev speed
    },
    {
        "Speed": 150,
        "time": datetime_obj + timedelta(microseconds=240),
        "X": 5,
        "Y": 15,
        "delta_time": datetime.timedelta(microseconds=190),
        "delta_speed": 50,
        "lateral_acceleration": 1.8237004563850359,  # note slight precision diff vs excel: 1.82370045638504
        "longitudinal_acceleration": 73099.41520467836,
    },
    {
        "Speed": 50,
        "time": datetime_obj + timedelta(microseconds=340),
        "X": 0,
        "Y": 5,
        "delta_time": datetime.timedelta(microseconds=100),
        "delta_speed": -100,
        "lateral_acceleration": 1.0912141684977585,
        "longitudinal_acceleration": -277777.77777777775,
    },
]
expected_df = pl.LazyFrame(data_exp)


def assertion_df_equality(got_df: pl.LazyFrame, expected_df):
    try:
        assert got_df.collect().equals(expected_df.collect())
    except AssertionError as e:
        print("Got:")
        print(got_df.collect())
        print("Expected:")
        print(expected_df.collect())
        got_df.collect().write_parquet("got.parquet")
        expected_df.collect().write_parquet("exp.parquet")
        raise AssertionError("Dataframes not matching") from e


def test_enrich_fastf1_telemetry():
    got_df = enrich_fastf1_telemetry(context, df)
    assertion_df_equality(got_df, expected_df)
