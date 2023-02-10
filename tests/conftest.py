"""Fixtures for tests"""
from time import sleep
import asyncio
import pytest
from fmu.sumo.uploader import CaseOnDisk, SumoConnection
from sumo.wrapper import SumoClient
from sumo.table_aggregation import utilities as ut

# These need to be reactivate if you want to rerun making of metadata for case
# import pyarrow as pa
# from pathlib import Path
# from fmu.config.utilities import yaml_load
# from fmu.dataio import InitializeCase, ExportData
# import pyarrow.feather as pf


@pytest.fixture(name="sumo", scope="session")
def fixture_sumo(sumo_env="prod"):
    """Return the sumo client to use
    args:
    sumo_env (str): name f sumo environment
    """
    return SumoClient(sumo_env)


@pytest.fixture(name="sumo_conn", scope="session")
def fixture_sumo_conn(sumo_env="prod"):
    """Return the sumo client to use
    args:
    sumo_env (str): name f sumo environment
    """
    return SumoConnection(env=sumo_env)


@pytest.fixture(name="case_metadata_path", scope="session")
def fixture_case_meta():
    """Return path to case metadata for dummy case

    Returns:
        str: path to case metadata
    """
    # test_path = Path("data/testrun/")
    # global_vars = yaml_load(test_path / "global_variables.yml")
    # If you ever have to remake fmu_case.yml
    # case = InitializeCase(config=global_vars)
    # path = case.export(
    # rootfolder=test_path,
    # casename="test-table-aggregation",
    # caseuser="dbs",
    # restart_from=None,
    # description=None,
    # force=True,
    # )

    # for file_path in [
    # path.resolve() for path in test_path.glob("realization-*/iter-*")
    # ]:
    # print(file_path)
    # os.chdir(file_path)
    # exp = ExportData(
    # tagname="eclipse",
    # content="timeseries",
    # config=global_vars,
    # verbosity="WARNING",
    # )
    # internal_path = Path("share/results/tables")
    # for arrow_file in internal_path.glob("*.arrow"):

    # table = pa.Table.from_pandas(pf.read_feather(arrow_file))
    # ind_path = exp.export(table, name="summary")
    # print(ind_path)

    path = "data/testrun/share/metadata/fmu_case.yml"
    return path


@pytest.fixture(name="case_name", scope="session")
def fixture_name():
    """Return case name

    Returns:
        str: name of test case in sumo
    """
    return "test-table-aggregation"


@pytest.fixture(name="case_uuid", scope="session")
def fixture_case(case_metadata_path, sumo_conn):
    """Return case uuid

    Args:
        case_metadata_path (str): path to metadatafile
        sumo_conn (SumoConnection): Connection to given sumo environment

    Returns:
        str: case uuid
    """
    case = CaseOnDisk(
        case_metadata_path=case_metadata_path,
        sumo_connection=sumo_conn,
        verbosity="DEBUG",
    )
    # Register the case in Sumo
    sumo_id = case.register()

    case.add_files(
        search_string="data/testrun/realization-*/iter-*/share/results/tables/*.arrow"
    )
    case.upload()
    print("Case registered on Sumo with ID: %s", sumo_id)

    # Prevent the tests from failing because upload is not done
    sleep(2)
    return sumo_id


@pytest.fixture(name="query_results", scope="session")
def fixture_query_results(sumo, case_name, name="summary"):
    """Return result for test run
    args:
    sumo (SumoClient instance): the client to use
    case_name (str): name of case
    name (str): name of table
    """
    query_results = ut.query_sumo(
        sumo, case_name, name, "iter-0", tag="eclipse", content="timeseries"
    )
    return query_results


@pytest.fixture(name="ids_and_friends", scope="session")
def fixture_ids_and_friends(query_results):
    """Returns results from given
    args:
    """
    return ut.get_blob_ids_w_metadata(query_results)


@pytest.fixture(name="aggregated_table", scope="session")
def fixture_aggregation(ids_and_friends, sumo):
    """Return aggregated objects

    Args:
        ids_and_friends (dict): dictionary of results
        sumo (SumoClient): Sumo client initialised to given env

    Returns:
        _type_: _description_
    """
    ids = ids_and_friends[1]
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(ut.aggregate_arrow(ids, sumo, loop))


@pytest.fixture(name="teardown", autouse=True, scope="session")
def fixture_teardown(case_uuid, sumo):
    """Remove case when all tests are run

    Args:
    case_uuid (str): uuid of test case
    sumo (SumoClient): Client to given sumo environment
    """
    yield
    print("Killing object {case_uuid}!")
    path = f"/objects('{case_uuid}')"

    sumo.delete(path)
