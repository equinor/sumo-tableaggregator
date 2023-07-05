"""Tests module _utils.py"""
import logging
from time import sleep
from uuid import UUID
import pyarrow as pa
from sumo.table_aggregation import utilities as ut
import yaml

logging.basicConfig(level="DEBUG", format="%(name)s %(levelname)s: %(message)s")

LOGGER = logging.getLogger()
LOGGER.setLevel("INFO")


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


def test_split_list():
    correct_len = 21
    test_list = ut.split_list(list(range(102)), 5)
    actual_len = len(test_list)
    assert (
        actual_len == correct_len
    ), f"Length of list is {actual_len}, but should be {correct_len}"


def test_query_input(query_input):
    """Tests query results"""
    results = query_input["hits"]["hits"]
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


def test_return_uuid(sumo, case_name, case_uuid):
    """Check function return_uuid

    Args:
        sumo (SumoClient): Client object with given environment
        case_name (str): The case uuid
        case_uuid (str): The case name
    """
    for identifier in (case_name, case_uuid):
        returned = ut.return_uuid(sumo, identifier)
        mess = f"{returned} not equal to {case_uuid}"
        assert returned == case_uuid, mess


def test_query_iterations(sumo, case_uuid):
    """Test query for iteration

    Args:
        sumo (SumoClient): Client object with given environment
        case_uuid (str, optional): Name of case to interrogate
    """
    print(case_uuid)
    results = ut.query_sumo_iterations(sumo, case_uuid)
    answer = ["iter-0"]
    assert results == answer


def test_get_blob_ids_w_metadata(ids_and_friends):
    """test results of function blob_ids_w_metadata
    args:
    ids_and_friends (tuple):  results from function
    """
    # (parent_id, blob_ids, agg_meta, meta.real_ids, meta.parameter_dict)
    print(ids_and_friends)
    parent_id, object_ids, meta, table_index = ids_and_friends
    assert isinstance(parent_id, str)
    assert_uuid_dict(object_ids)
    assert isinstance(meta, dict), f"Meta is not a dict, {type(meta)}"
    assert isinstance(table_index, (list, type(None)))


def test_aggregation(aggregated_table):
    """Tests function agggregate_objects
    args:
    ids_and_friends (tuple): results from function blob_ids_w_metadata
    sumo (SumoClient instance): the client to use during aggregation
    """
    correct_shape = (20, 5)

    table_shape = aggregated_table.shape
    assert isinstance(aggregated_table, pa.Table), "Not a pyarrow table"
    assert table_shape == correct_shape, "Wrong shape"


def test_upload(
    do_upload, query_results, case_name, sumo, ids_and_friends, aggregated_table
):
    """Upload data to sumo"""
    # Define paths to check against
    operations = ("collection", "mean", "min", "max", "p10", "p90")
    valids = ["FOPP", "FOPT", "FOPR"]
    all_names = [
        f"summary--{name}--eclipse--{op}--iter-0"
        for name in valids
        for op in operations
    ]
    # Run upload
    do_upload(sumo, ids_and_friends, aggregated_table)
    sleep(5)
    hits = query_results(sumo, case_name)
    print(f"Found  {len(hits)} aggregations")
    unique_count = {name: 0 for name in all_names}
    for result in hits:
        correct_len = 3
        meta = result["_source"]
        name = meta["data"]["name"]
        print(f"data.name: {name}")
        assert name == "summary", f"Name is not summary but {name}"
        operation = meta["fmu"]["aggregation"]["operation"]
        print(f"fmu.aggregation.name: {operation}")
        columns = meta["data"]["spec"]["columns"]
        print(f"data.spec.columns: {columns}")
        rel_path = meta["file"]["relative_path"]
        print(f"file.relative_path: {rel_path}")
        unique_count[rel_path] += 1
        index_names = meta["data"]["table_index"]
        print(index_names)
        print(columns)
        col_len = len(columns)
        # For statistical aggregations
        if len(index_names) == 1:
            correct_len = 2
        assert (
            col_len == correct_len
        ), f"Length of columns != {correct_len} ({col_len}) and cols are {columns}"
        if col_len == 2:
            col_name = columns.pop()
            if "DATE" not in index_names:
                assert col_name in valids, f"Column name {col_name} is invalid"
        assert operation in operations, f"Operation {operation} is invalid"
        table = ut.get_object(result["_id"], sumo)
        print(f"{name}-{operation}: {table.column_names}")
        print("---------")
        print(columns)
        print("---------")

    # Check if all objects are made
    missing = []
    total_count = 0
    for rel_path, count in unique_count.items():
        if count == 0:
            missing.append(rel_path)
        else:
            total_count += 1
    miss_mess = "\n".join(missing)
    assert (
        len(missing) == 0
    ), f"These paths are missing {miss_mess} (found {total_count})"


def assert_reals(real_dict):
    """Test if keys can be made into int's in dict

    Args:
        real_dict (dict): dict to check
    """
    for real_nr in real_dict.keys():
        assert isinstance(int(real_nr), int), f"{real_nr} cannot be converted to int"


def test_parameter_dict(query_results, sumo, case_name):
    """Test the parameter dictionary generated

    Args:
        query_results (dict): list of hist from sumo query
        sumo (SumoClient): client to given sumo environment
        case_name (str): name of case
    """
    p_dict = query_results(sumo, case_name)[0]["_source"]["fmu"]["iteration"][
        "parameters"
    ]

    assert len(p_dict) == 3
    for group_name, group_dict in p_dict.items():
        if group_name == "FILE":
            assert_reals(group_dict)
        else:
            # print(group_dict.keys())
            # print(group_dict.values())
            for key, value in group_dict.items():
                print(key)
                assert_reals(value)

    #     assert isinstance(
    #         group_dict, dict
    #     ), f"Group {group_name} is not dict but {type(group_dict)}"
    #     for var_name, var_dict in group_dict.items():
    #         assert all(
    #             isinstance(real_nr, int) for real_nr in var_dict
    #         ), f"Not all real_nrs for {var_name} are int {group_dict.keys()}"
