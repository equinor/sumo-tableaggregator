"""Tests for module dispath"""
import time
import pytest
from sumo.table_aggregation.utilities import timethis
from sumo.table_aggregation import dispatch


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


def test_collect_case_table_name_and_tag(uuid, sumo):
    """Test function collect_case_table_name_and_tag

    Args:
        uuid (str): uuid of case
        sumo (SumoClient): client to given environment
    """
    results = dispatch.collect_case_table_name_and_tag(uuid, sumo)
    print(results)


def test_generate_dispatch_info_per_combination(uuid, sumo):
    """Test function generate_dispatch_info_per_combination

    Args:
        uuid (str): uuid of case
        sumo (SumoClient): client to given environment
    """
    combination = (uuid, "SNORRE", "rft", "iter-0")
    results = dispatch.generate_dispatch_info_per_combination(combination, sumo)
    print(results)


# @timethis("dispatchlist")
def test_generate_dispatch_info(uuid):
    """Test of function generate_dispatch info

    Args:
        uuid (str): uuid of case
    """
    nr_batch_jobs = 45
    start = time.perf_counter()
    results = dispatch.generate_dispatch_info(uuid, "prod")
    stop = time.perf_counter()
    print("--> Timex (%s): %s s", round(stop - start, 2))
    found_len = len(results)
    assert found_len == nr_batch_jobs, f"found {found_len}, should be {nr_batch_jobs}"
