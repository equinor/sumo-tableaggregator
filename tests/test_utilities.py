"""Tests module _utils.py"""
import sys

from pathlib import Path
import json
from uuid import UUID
import pandas as pd
import pyarrow as pa
from pyarrow import feather

# import pytest

from sumo.table_aggregation import utilities as ut


# TEST_DATA = Path(__file__).parent / "data"
# TEST_ARROW_FILE = TEST_DATA / "2_columns_data.arrow"
# AGGREGATED_CSV = TEST_DATA / "aggregated.csv"
# MINIAGG_CSV = TEST_DATA / "mini_aggregated.csv"
# QUERY_FILE = TEST_DATA / "query_results.json"
# TMP = Path("tmp")


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
        assert isinstance(key, int), assert_mess
        assert_correct_uuid(uuid_dict[key])


def assert_name(name, func_name, typ):
    """Runs assert statement for test_decide_name
    args:
    name (str): correct value
    func_name (str): return value from function
    typ(str): type to be printed
    """
    print(f"{typ}: ")
    assert_mess = f"{typ} gives gives wrong value ({func_name} vs {name})"
    assert name == func_name, assert_mess


# def read_json(input_file):
#     """read json from disc
#     args:
#     result_file (str): path to file relative to TEST_DATA
#     returns:
#     content (dict): results from file
#     """
#     with open(input_file, "r", encoding="utf-8") as json_file:
#         contents = json.load(json_file)
#     return contents


def test_query_results(query_results):
    """Tests query results"""
    results = query_results["hits"]["hits"]
    assert len(results) == 4, "Not the expected number of hits"
    # result_path = QUERY_FILE
    # write_json(result_path, query_results)


def test_query_iterations(sumo, case_name="drogon_ahm_dev-2023-01-18"):
    """Test query for iteration

    Args:
        sumo (SumoClient): Client object with given environment
        case_name (str, optional): Name of case to interrogate. Defaults to "drogon_ahm_dev-2023-01-18".
    """
    results = ut.query_sumo_iterations(sumo, case_name)
    answer = [0, 1]
    assert results == answer


def test_decide_name():
    """Tests function decide_name in utils"""
    name = "tudels"
    assert_name(name, ut.decide_name(name), "str")

    namer = ["REAL", name]
    assert_name(name, ut.decide_name(namer), "list")

    namer.append("BULK")
    assert_name("aggregated_volumes", ut.decide_name(namer), "Vol list")

    namer.remove("BULK")
    namer.append("Haya")
    assert_name("aggregated_summary", ut.decide_name(namer), "Bulk less list")

    frame = pd.DataFrame({"REAL": [1, 2, 3], name: [4, 9, 0]})
    assert_name(name, ut.decide_name(frame.columns), "pd.DataFrame.index")


def test_get_blob_ids_w_metadata(ids_and_friends):
    """testing return results of function blob_ids_w_metadata
    args:
    ids_and_friends (tuple):  results from function
    """
    parent_id, object_ids, meta, real_ids, p_dict = ids_and_friends
    assert isinstance(parent_id, str)
    assert_uuid_dict(object_ids)
    assert isinstance(meta, dict), f"Meta is not a dict, {type(meta)}"
    ass_mess = f"Real ids are not tuple, or list {type(real_ids)}"
    assert all(isinstance(num, int) for num in real_ids), "some reals are not int"
    assert isinstance(real_ids, (tuple, list)), ass_mess
    assert isinstance(p_dict, dict), f"p_dict is not dict, {type(p_dict)}"


def test_stats_from_aggregated(agg_dummy):
    """Test generation of statistics from an aggregated like table

    Args:
        agg_dummy (pa.Table): mock data of aggregated table
    """
    answer = ["mean", "min", "max", "p10", "p90", "DATE"]
    print(agg_dummy)
    stats = ut.make_stat_aggregations(agg_dummy, "v1", ["DATE"], {})

    col_names = stats.column_names
    assert isinstance(stats, pa.Table), "Wrong data type"
    assert col_names == answer, "Wrong column names"


# def test_aggregation(ids_and_friends, sumo):
#     """Tests function agggregate_objects
#     args:
#     ids_and_friends (tuple): results from function blob_ids_w_metadata
#     sumo (SumoClient instance): the client to use during aggregation
#     """
#     ids = ids_and_friends[1]
#     results = ut.aggregate_arrow(ids, sumo)
#     assert isinstance(results, pa.Table)


# def test_store_aggregated_objects(ids_and_friends, sumo):
#     """Tests function store_aggregregated_results
#     args:
#     file_name (str, or posix path): file to read from
#     """
#     ids, meta = ids_and_friends[1:3]
#     agg_frame = ut.aggregate_arrow(ids, sumo)
#     ut.store_aggregated_objects(agg_frame, meta, 0)
#     assert_file_and_meta_couples(TMP)


# def test_make_stat_aggregations(file_name=MINIAGG_CSV):
#     """Tests function store_stat_aggregations"""
#     frame = pd.read_csv(file_name)
#     print(frame.head())
#     stats = ut.make_stat_aggregations(frame)
#     print(stats["FGPR"]["mean"])


# def test_upload_aggregated(sumo, store_folder=TMP):
#     """Tests function upload aggregated
#     args:
#     sumo (SumoClient instance): the client to use for uploading
#     store_folder (str): folder containing results
#     """
#     count = ut.upload_aggregated(
#         sumo, "17c56e33-38cd-f8d4-3e83-ec2d16a85327", store_folder
#     )
#     assert count == 4, f"Not uploaded all files ({count})"


# def test_get_object(sumo):
#     """Testing getting of object"""
#     object_id = "ce25b1c1-6633-4a13-101a-863de9e85c1d"
#     ut.get_object(object_id, sumo)


def test_table_to_bytes(arrow_table):
    """Tests generation of bytestring from table"""
    # # frame = pd.DataFrame({"test": [1, 2, 3]})
    # table = pa.Table.from_pandas(frame)
    byte_string = ut.table_to_bytes(arrow_table)
    assert isinstance(byte_string, bytes), "Arrow table not converted to bytes  "


if __name__ == "__main__":
    print(sys.path)
    print(dir(ut))
