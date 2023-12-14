from sumo.wrapper import SumoClient
from sumo.table_aggregation import dispatch
import pytest
import logging
import json
from pathlib import Path

logging.basicConfig(level="DEBUG")


@pytest.fixture(name="uuid", scope="module")
def fix_uuid():
    """Return case uuid

    Returns:
        str: uuid of case
    """
    return "9c9d9a52-1cf4-44cc-829f-23b8334ae813"
    # return "dfac6a1b-c6a4-408a-94a1-cb292693da97"


@pytest.fixture(name="sumo", scope="module")
def fix_sumo(case_env="preview"):
    """Return client for given environment

    Args:
        case_env (str, optional): name of environment. Defaults to "prod".

    Returns:
        SumoClient: the client for given environment
    """
    return SumoClient(case_env)


@pytest.fixture(name="pit", scope="module")
def fix_pit(sumo):
    pit = sumo.post("/pit", params={"keep-alive": "1m"}).json()
    print("------------")
    print(pit)
    print("------------")

    return pit["id"]


def test_query_for_it_name_and_tags(uuid, sumo, pit):
    """Test function query_for_it_name_and_tags

    Args:
        uuid (str): case uuid
        sumo (SumoClient): Client for given environment
        pit (sumo.pit): point in time for store
    """
    print(dispatch.query_for_names_and_tags(sumo, uuid, pit))



def test_collect_it_name_and_tag(sumo, uuid, pit):
    """Test function collect_it_name_and_tag

    Args:
        uuid (str): case uuid
        sumo (SumoClient): Client for given environment
        pit (sumo.pit): point in time for store
    """
    print("-------")
    print(dispatch.collect_names_and_tags(sumo, uuid, pit))


def test_list_of_list_segments(sumo, uuid, pit):
    results = dispatch.list_of_list_segments(
        sumo, uuid, "SNORRE", "summary", ["DATE"], pit
    )
    correct_segments = 32
    actual_segments = len(results)
    fopt_found = False
    for result in results:
        if "FOPT" in result:
            fopt_found = True
    print(len(results))
    assert fopt_found, "NO FOPT found in segments"
    assert (
        actual_segments == correct_segments
    ), f"Actual segments: {actual_segments}, should be {correct_segments}"


def test_generate_dispatch_info(uuid, env="preview"):
    """Test function generate_dispatch_info

    Args:
        uuid (str): case uuid
        env (str, optional): sumo environment. Defaults to "prod".
    """
    tasks = dispatch.generate_dispatch_info(uuid, env, "iter-1")
    for task in tasks:
        assert len(task["columns"]) <= len(task["table_index"]) + 250, "Too many columns in task"
        assert len(task["columns"]) > len(task["table_index"]), "more columns than table index"
        assert len(task["base_meta"]["data"]["spec"]["columns"]) == 0, f"data.spec.columns not nulled ({len(task['base_meta']['data']['spec']['columns'])})"
        print(len(task["base_meta"]["data"]["spec"]["columns"]))

    assert len(tasks) == 104, "Should be 104 jobs for Troll case"



def test_aggregate_and_upload(sumo):
    json_file = "data/dispath_info_snorre.json"

    content = []
    with open(Path(__file__).parent / json_file, "r", encoding="utf-8") as stream:
        content = json.load(stream)
    print(content[0])
    dispatch.aggregate_and_upload(content[0], sumo)
