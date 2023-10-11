from sumo.wrapper import SumoClient
from sumo.table_aggregation import dispatch as disp
from sumo.table_aggregation import dispatch
import pytest
import logging

logging.basicConfig(level="DEBUG")


@pytest.fixture(name="uuid", scope="module")
def fix_uuid():
    """Return case uuid

    Returns:
        str: uuid of case
    """
    return "4582d741-ee41-485b-b2ea-912a7d7dc57c"
    # return "dfac6a1b-c6a4-408a-94a1-cb292693da97"


@pytest.fixture(name="sumo", scope="module")
def fix_sumo(case_env="test"):
    """Return client for given environment

    Args:
        case_env (str, optional): name of environment. Defaults to "prod".

    Returns:
        SumoClient: the client for given environment
    """
    return SumoClient(case_env)


@pytest.fixture(name="pit", scope="module")
def fix_pit(sumo):
    return sumo.post("/pit", params={"keep-alive": "5m"}).json()["id"]


def test_query_for_it_name_and_tags(uuid, sumo, pit):
    """Test function query_for_it_name_and_tags

    Args:
        uuid (str): case uuid
        sumo (SumoClient): Client for given environment
        pit (sumo.pit): point in time for store
    """
    print(disp.query_for_it_name_and_tags(sumo, uuid, pit))


def test_query_for_columns(sumo, uuid, pit):
    """Test function query_for_columns

    Args:
        uuid (str): case uuid
        sumo (SumoClient): Client for given environment
        pit (sumo.pit): point in time for store

    """
    # results = disp.query_for_columns(sumo, uuid, "SNORRE", "summary", pit)
    results = disp.query_for_columns(sumo, uuid, "geogrid", "vol", pit)
    print(results)
    # print("FOPT" in results)


def test_collect_it_name_and_tag(sumo, uuid, pit):
    """Test function collect_it_name_and_tag

    Args:
        uuid (str): case uuid
        sumo (SumoClient): Client for given environment
        pit (sumo.pit): point in time for store
    """
    print("-------")
    print(disp.collect_it_name_and_tag(sumo, uuid, pit))


def test_logging():
    logger = logging.getLogger("Mine")
    logger.debug("Hei")


def test_generate_dispatch_info(uuid, env="prod"):
    """Test function generate_dispatch_info

    Args:
        uuid (str): case uuid
        env (str, optional): sumo environment. Defaults to "prod".
    """
    tasks = disp.generate_dispatch_info(uuid, env)
    print(tasks)
    # print(f"{len(tasks)} element created")
    # # the_essentials = ["FOPT", "FOPR", "FGPT", "FGPR"]
    # fopt_found = False
    # prev_list = []
    # mandatories = ["table_index", "columns", "object_ids", "base_meta"]
    # all_mandatories_found = False
    # lists_equal = False
    # for task in tasks:
    #     if "FOPT" in task["columns"]:
    #         fopt_found = True
    #     for mandatory in mandatories:
    #         all_mandatories_found = mandatory in task.keys()
    #     if prev_list == task["columns"]:
    #         lists_equal = True
    #     prev_list = task["columns"]


#
# assert fopt_found, "FOPT not found"
# assert all_mandatories_found, "Some of the mandatories not found"
# # assert not lists_equal, "Some list elements are equal"
# assert len(tasks) == 47, "Wrong length!"
