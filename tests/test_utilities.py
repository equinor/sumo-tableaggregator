"""Tests module _utils.py"""
import pandas as pd
import pyarrow as pa
from pathlib import Path
from pyarrow import feather
from sumo.wrapper import SumoClient
from context import ut
import pytest


TEST_ARROW_FILE = Path(__file__).parent / "data/2_columns_data.arrow"

print(TEST_ARROW_FILE)


@pytest.fixture
def sumo(sumo_env="prod"):
    """Returns a the sumo client to use
    """
    return SumoClient(sumo_env)


@pytest.fixture
def pandas_frame():

    """Defines pandas dataframe to be used in tests"""
    indata = {"nums": [1, 2, 3], "letters": ["A", "B", "C"]}

    return pd.DataFrame(indata)


@pytest.fixture
def arrow_table(pandas_frame):
    """Makes pyarrow table from pandas dataframe
    args:
    frame (pd.DataFrame): the dataframe to convert
    returns: table (pa.Table): frame as pa.Table
    """
    print(TEST_ARROW_FILE)
    schema = pa.Schema.from_pandas(pandas_frame)
    table = pa.Table.from_pandas(pandas_frame, schema=schema)
    feather.write_feather(table, dest=TEST_ARROW_FILE)
    return TEST_ARROW_FILE


def test_read_arrow_to_frame(pandas_frame, arrow_table, sumo):
    """tests function arrow_to_frame
    args:
    pandas_frame (pd.DataFrame): to check against
    arrow_table (str): name of file
    """

    check_table = ut.arrow_to_frame(arrow_table)
    assert check_table.equals(pandas_frame)


if __name__ == "__main__":
    print(TEST_ARROW_FILE)
