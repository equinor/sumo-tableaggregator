"""Tests module _utils.py"""
from time import sleep
from uuid import UUID
import asyncio
from concurrent.futures import ThreadPoolExecutor
import pyarrow as pa
from sumo.table_aggregation import utilities as ut


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
        case_name (str, optional): Name of case to interrogate
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
    ids_and_friends,
    case_name,
    aggregated_table,
    sumo,
):
    """Upload data to sumo"""
    executor = ThreadPoolExecutor()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        ut.extract_and_upload(
            sumo,
            ids_and_friends[0],
            aggregated_table,
            ["DATE"],
            ids_and_friends[2],
            loop,
            executor,
        )
    )
    # Prevent tests from failing because upload is not done
    sleep(5)
    result_query = sumo.get(
        "/search",
        query=f"fmu.case.name:{case_name} AND class:table AND fmu.aggregation:*",
        size=100,
    )
    hits = result_query["hits"]["hits"]
    correct_nr = 20
    print(f"Found  {len(hits)} aggregations")
    operations = ("collection", "mean", "min", "max", "p10", "p90")
    valids = ["FOPP", "FOPT", "FOPR"]
    all_names = [
        f"summary--{name}--eclipse--{op}--iter-0"
        for name in valids
        for op in operations
    ]
    all_names.extend(
        [
            "summary--table_index--eclipse--mean--iter-0",
            "summary--table_index--eclipse--collection--iter-0",
        ]
    )
    unique_count = {name: 0 for name in all_names}
    for result in hits:
        correct_len = 1
        meta = result["_source"]
        name = meta["data"]["name"]
        print(f"data.name: {name}")
        assert name == "summary", f"Name is not summary but {name}"
        operation = meta["fmu"]["aggregation"]["operation"]
        print(f"fmu.aggregation.name: {operation}")
        columns = meta["data"]["spec"]["columns"]
        print(f"data.spec.columns: {columns}")
        print(f"file.relative_path: {meta['file']['relative_path']}")
        index_names = meta["data"]["table_index"]
        unique_count[meta["file"]["relative_path"]] = (
            unique_count[meta["file"]["relative_path"]] + 1
        )
        if "DATE" in columns:
            if operation == "collection":
                correct_len = 2
                comb_index = True

        col_len = len(columns)
        assert (
            col_len == correct_len
        ), f"Length of columns != 1 ({col_len}) and cols are {columns}"
        if col_len == 1:
            col_name = columns.pop()
            if "DATE" not in index_names:
                assert col_name in valids, f"Column name {col_name} is invalid"
        assert operation in operations, f"Operation {operation} is invalid"
        table = ut.get_object(result["_id"], sumo)
        print(f"{name}-{operation}: {table.column_names}")
        print(columns)
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
