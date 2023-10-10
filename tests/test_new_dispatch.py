from sumo.wrapper import SumoClient
from sumo.table_aggregation import new_dispatch as nd
from sumo.table_aggregation import dispatch
import pytest


@pytest.fixture(name="uuid", scope="module")
def fix_uuid():
    """Return case uuid

    Returns:
        str: uuid of case
    """
    return "4582d741-ee41-485b-b2ea-912a7d7dc57c"


@pytest.fixture(name="sumo", scope="module")
def fix_sumo(case_env="prod"):
    """Return client for given environment

    Args:
        case_env (str, optional): name of environment. Defaults to "prod".

    Returns:
        SumoClient: the client for given environment
    """
    return dispatch.init_sumo_env(case_env)


@pytest.fixture(name="pit", scope="module")
def fix_pit(sumo):
    return sumo.post("/pit", params={"keep-alive": "5m"}).json()["id"]


def test_query(uuid, sumo, pit):
    nd.query_for_it_name_and_tags(sumo, uuid, pit)


def test_query_for_columns(sumo, uuid, pit):
    print("FOPT" in nd.query_for_columns(sumo, uuid, "SNORRE", "summary", pit))


def test_collect_it_name_and_tag(sumo, uuid, pit):
    print("-------")
    print(nd.collect_it_name_and_tag(sumo, uuid, pit))


def test_generate_dispatch_info(uuid, env="prod"):
    tasks = nd.generate_dispatch_info(uuid, env)
    print(f"{len(tasks)} element created")
    # the_essentials = ["FOPT", "FOPR", "FGPT", "FGPR"]
    fopt_found = False
    prev_list = []
    lists_equal = False
    for task in tasks:
        if "FOPT" in task["columns"]:
            fopt_found = True
        if prev_list == task["columns"]:
            lists_equal = True
        prev_list = task["columns"]

    assert fopt_found, "FOPT not found"
    # assert not lists_equal, "Some list elements are equal"
    assert len(tasks) == 47, "Wrong length!"
