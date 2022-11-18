"""Tests module _utils.py"""
import pandas as pd
import pyarrow as pa
from pathlib import Path
from pyarrow import feather
from uuid import UUID
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


def assert_correct_uuid(uuid_to_check, version=4):
    """Checks if uuid has correct structure
    args:
    uuid_to_check (str): to be checked
    version (int): what version of uuid to compare to
    """
    # Concepts stolen from stackoverflow.com
    # questions/19989481/how-to-determine-if-a-string-is-a-valid-v4-uuid
    type_mess = f"{uuid_to_check} is not str ({type(uuid_to_check)}"
    assert isinstance(uuid_to_check, str), type_mess
    works_for_me = True
    try:
        UUID(uuid_to_check, version=version)
    except ValueError:
        works_for_me = False
    structure_mess = f"{uuid_to_check}, does not have correct structure"
    assert works_for_me, structure_mess


def assert_uuid_dict(uuid_dict):
    """Tests that dict has string keys, and valid uuid's as value
    args:
    uuid_dict (dict): dict to test
    """
    for key in uuid_dict:
        assert_mess = f"{key} is not of type str"
        assert isinstance(key, str), assert_mess
        assert_correct_uuid(uuid_dict[key])


def test_read_arrow_to_frame(pandas_frame, arrow_table, sumo):
    """tests function arrow_to_frame
    args:
    pandas_frame (pd.DataFrame): to check against
    arrow_table (str): name of file
    """

    check_table = ut.arrow_to_frame(arrow_table)
    assert check_table.equals(pandas_frame)


def test_get_blob_ids_w_metadata(sumo):
    results = ut.get_blob_ids_w_metadata(sumo, "drogon_design_2022_11-01",
                                         "summary")
    object_ids, meta, real_ids, p_dict = results
    assert_uuid_dict(object_ids)
    assert isinstance(meta, dict), f"Meta is not a dict, {type(meta)}"
    assert isinstance(real_ids, tuple), f"Real ids are not tuple, {type(real_ids)}"
    assert isinstance(p_dict, dict), f"p_dict is not dict, {type(p_dict)}"


if __name__ == "__main__":
    print(TEST_ARROW_FILE)
