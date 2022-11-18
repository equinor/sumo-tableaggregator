"""Tests module _utils.py"""
from pathlib import Path
import json
from uuid import UUID
import pandas as pd
import pyarrow as pa
from pyarrow import feather
from sumo.wrapper import SumoClient
import pytest
from context import ut, Timer


TIMER = Timer()

TEST_DATA = Path(__file__).parent / "data"
TEST_ARROW_FILE = TEST_DATA / "2_columns_data.arrow"
AGGREGATED_CSV = TEST_DATA / "aggregated.csv"
MINIAGG_CSV = TEST_DATA / "mini_aggregated.csv"
QUERY_FILE = TEST_DATA /"query_results.json"
TMP = Path("tmp")


@pytest.fixture(name="sumo")
def fixture_sumo(sumo_env="prod"):
    """Returns a the sumo client to use
    args:
    sumo_env (str): what environment to use
    """
    return SumoClient(sumo_env)


@pytest.fixture(name="query_results")
def fixture_query_results(sumo, case_name="drogon_design_2022_11-01",
                          name="summary"):
    """Returns results from given
    args:
    sumo (SumoClient instance): the client to use
    case_name (str): name of string
    name (str): name of files
    """
    query_results = ut.query_sumo(sumo, case_name, name)
    return query_results


def test_query_results(query_results):
    """Tests query results"""
    print(query_results)
    result_path = QUERY_FILE
    write_json(result_path, query_results)


@pytest.fixture(name="ids_and_friends")
def fixture_ids_and_friends(query_file=QUERY_FILE):
    """Returns results from given
    args:
    """
    query_results = read_json(query_file)
    return ut.get_blob_ids_w_metadata(query_results)


def write_json(result_file, results):
    """writes json files to disc
    args:
    result_file (str): path to file relative to TEST_DATA
    """
    with open(result_file, "w", encoding="utf-8") as json_file:
        json.dump(results, json_file)


def read_json(input_file):
    """read json from disc
    args:
    result_file (str): path to file relative to TEST_DATA
    returns:
    content (dict): results from file
    """
    with open(input_file, "r", encoding="utf-8") as json_file:
        contents = json.load(json_file)
    return contents


def assert_file_and_meta_couples(folder):
    """Checks that there are metadata for all files
    args:
    folder (str or PosixPath): the folder to get from
    """
    all_hits = list(Path(folder).glob("*"))
    print(len(all_hits))
    assert len(all_hits) % 2 == 0
    the_files = [hit.name for hit in all_hits if not hit.name.startswith(".")]
    the_metas = [
        hit.name[1:].replace(".yml", "")
        for hit in all_hits if hit.name.startswith(".")
    ]
    for name in the_metas:
        assert name in the_files, f"{name} does not have a corresponding file"
        the_files.remove(name)
    assert len(the_files) == 0, f"{the_files} does not have meta files"


@pytest.fixture(name="pandas_frame")
def fixture_pandas_frame():

    """Defines pandas dataframe to be used in tests"""
    indata = {"nums": [1, 2, 3], "letters": ["A", "B", "C"]}

    return pd.DataFrame(indata)


@pytest.fixture(name="arrow_table")
def fixture_arrow_table(pandas_frame):
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
        assert isinstance(key, int), assert_mess
        assert_correct_uuid(uuid_dict[key])


def test_read_arrow_to_frame(pandas_frame, arrow_table):
    """tests function arrow_to_frame
    args:
    pandas_frame (pd.DataFrame): to check against
    arrow_table (str): name of file
    """

    check_table = ut.arrow_to_frame(arrow_table)
    assert check_table.equals(pandas_frame)


def test_get_blob_ids_w_metadata(ids_and_friends):
    """testing return results of function blob_ids_w_metadata
    args:
    ids_and_friends (tuple):  results from function
    """
    object_ids, meta, real_ids, p_dict = ids_and_friends
    assert_uuid_dict(object_ids)
    assert isinstance(meta, dict), f"Meta is not a dict, {type(meta)}"
    ass_mess = f"Real ids are not tuple, or list {type(real_ids)}"
    assert all(isinstance(num, int) for num in real_ids), "some reals are not int"
    assert isinstance(real_ids, (tuple, list)), ass_mess
    assert isinstance(p_dict, dict), f"p_dict is not dict, {type(p_dict)}"


def test_aggregation(ids_and_friends, sumo):
    """Tests function agggregate_objects
    args:
    ids_and_friends (tuple): results from function blob_ids_w_metadata
    sumo (SumoClient instance): the client to use during aggregation
    """
    ids = ids_and_friends[0]
    TIMER.start()
    results = ut.aggregate_objects(ids, sumo)
    TIMER.stop()
    results.to_csv(AGGREGATED_CSV)
    # print(results.head())


def test_store_aggregated_objects(ids_and_friends, file_name=MINIAGG_CSV):
    """Tests function store_aggregregated_results
    args:
    file_name (str, or posix path): file to read from
    """
    meta_stub = ids_and_friends[1]
    frame = pd.read_csv(file_name)
    TIMER.start()
    ut.store_aggregated_objects(frame, meta_stub)
    TIMER.stop()
    assert_file_and_meta_couples(TMP)
