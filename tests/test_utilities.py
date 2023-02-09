"""Tests module _utils.py"""
from pathlib import Path
from uuid import UUID
import pandas as pd
import pyarrow as pa
from pyarrow import feather
from sumo.table_aggregation import utilities as ut
import asyncio


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


def test_parent_uuid(case_uuid):
    """Check case uuid

    Args:
        case_uuid (str): uuid case
    """
    assert_correct_uuid(case_uuid)


def test_query_results(query_results):
    """Tests query results"""
    results = query_results["hits"]["hits"]
    print(results)
    res_length = len(results)
    # Check length of results, there are only 4
    assert len(results) == 4, f"Not the expected number of hits: {res_length} not 4!"
    correct_name = "summary"
    for hit in results:
        meta = hit["_source"]
        found_name = meta["data"]["name"]
        mess = f"Name in metadata should be {correct_name} is {found_name}"
        assert found_name == correct_name, mess


def test_query_iterations(sumo, case_name):
    """Test query for iteration

    Args:
        sumo (SumoClient): Client object with given environment
        case_name (str, optional): Name of case to interrogate. Defaults to "drogon_ahm_dev-2023-01-18".
    """
    print(case_name)
    results = ut.query_sumo_iterations(sumo, case_name)
    answer = ["iter-0"]
    assert results == answer


def test_get_blob_ids_w_metadata(ids_and_friends):
    """test results of function blob_ids_w_metadata
    args:
    ids_and_friends (tuple):  results from function
    """
    # (parent_id, blob_ids, agg_meta, meta.real_ids, meta.parameter_dict)
    print(ids_and_friends)
    parent_id, object_ids, meta, real_ids, p_dict = ids_and_friends
    assert isinstance(parent_id, str)
    assert_uuid_dict(object_ids)
    assert isinstance(meta, dict), f"Meta is not a dict, {type(meta)}"
    ass_mess = f"Real ids are not tuple, or list {type(real_ids)}"
    assert all(isinstance(num, int) for num in real_ids), "some reals are not int"
    assert isinstance(real_ids, (tuple, list)), ass_mess
    assert isinstance(p_dict, dict), f"parameter_dict is not dict, {type(p_dict)}"


def test_aggregation(ids_and_friends, sumo):
    """Tests function agggregate_objects
    args:
    ids_and_friends (tuple): results from function blob_ids_w_metadata
    sumo (SumoClient instance): the client to use during aggregation
    """
    correct_shape = (20, 5)
    ids = ids_and_friends[1]
    loop = asyncio.get_event_loop()
    results = loop.run_until_complete(ut.aggregate_arrow(ids, sumo, loop))
    table_shape = results.shape
    assert isinstance(results, pa.Table), "Not a pyarrow table"
    assert table_shape == correct_shape, "Wrong shape"


# # def test_store_aggregated_objects(ids_and_friends, sumo):
# #     """Tests function store_aggregregated_results
# #     args:
# #     file_name (str, or posix path): file to read from
# #     """
# #     ids, meta = ids_and_friends[1:3]
# #     agg_frame = ut.aggregate_arrow(ids, sumo)
# #     ut.store_aggregated_objects(agg_frame, meta, 0)
# #     assert_file_and_meta_couples(TMP)


# # def test_make_stat_aggregations(file_name=MINIAGG_CSV):
# #     """Tests function store_stat_aggregations"""
# #     frame = pd.read_csv(file_name)
# #     print(frame.head())
# #     stats = ut.make_stat_aggregations(frame)
# #     print(stats["FGPR"]["mean"])


# # def test_upload_aggregated(sumo, store_folder=TMP):
# #     """Tests function upload aggregated
# #     args:
# #     sumo (SumoClient instance): the client to use for uploading
# #     store_folder (str): folder containing results
# #     """
# #     count = ut.upload_aggregated(
# #         sumo, "17c56e33-38cd-f8d4-3e83-ec2d16a85327", store_folder
# #     )
# #     assert count == 4, f"Not uploaded all files ({count})"


# # def test_get_object(sumo):
# #     """Testing getting of object"""
# #     object_id = "ce25b1c1-6633-4a13-101a-863de9e85c1d"
# #     ut.get_object(object_id, sumo)


# def test_table_to_bytes(arrow_table):
#     """Tests generation of bytestring from table"""
#     # # frame = pd.DataFrame({"test": [1, 2, 3]})
#     # table = pa.Table.from_pandas(frame)
#     byte_string = ut.table_to_bytes(arrow_table)
#     assert isinstance(byte_string, bytes), "Arrow table not converted to bytes  "


# if __name__ == "__main__":
#     print(sys.path)
#     print(dir(ut))
