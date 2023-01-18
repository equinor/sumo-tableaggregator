"""Fixtures for tests"""
import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from sumo.wrapper import SumoClient

from sumo.table_aggregation import utilities as ut


@pytest.fixture(name="sumo")
def fixture_sumo(sumo_env="dev"):
    """Return the sumo client to use
    args:
    sumo_env (str): name f sumo environment
    """
    return SumoClient(sumo_env)


@pytest.fixture(name="query_results")
def fixture_query_results(
    sumo, case_name="drogon_design_small-2023-01-18", name="summary"
):
    """Returns result for given case
    args:
    sumo (SumoClient instance): the client to use
    case_name (str): name of case
    name (str): name of table
    """
    query_results = ut.query_sumo(sumo, case_name, name, 0, content="depth")
    return query_results


@pytest.fixture(name="ids_and_friends")
def fixture_ids_and_friends(query_results):
    """Returns results from given
    args:
    """
    return ut.get_blob_ids_w_metadata(query_results)


@pytest.fixture(name="agg_dummy")
def fixture_agg_dummmy():
    """Generate mock data for aggregated table

    Returns:
        pa.Table: mock aggregated table
    """
    nr_rows = 6000
    vectors = np.random.normal(0, 1, 2 * nr_rows).reshape(nr_rows, 2)
    real = np.sort(np.random.choice([1, 2, 3], nr_rows, p=[0.33, 0.33, 0.34]))
    date = np.tile(np.arange(nr_rows / 3), (1, 3)).reshape(
        nr_rows,
    )
    print(vectors.shape)
    print(real.shape)
    print(date.shape)
    frame = pd.DataFrame(
        {
            "v1": vectors[:, 0].flatten(),
            "v2": vectors[:, 1].flatten(),
            "REAL": real,
            "DATE": date,
        }
    )
    table = pa.Table.from_pandas(frame)
    return table


@pytest.fixture(name="agg_frame")
def fixture_agg_frame(sumo, ids_and_friends):
    """Return aggregated frame
    args:
    ids (dict): dictionary with name as key, value object id
    returns frame (pd.DataFrame)
    """
    ids = ids_and_friends[1]
    results = ut.aggregate_arrow(ids, sumo)
    return results


@pytest.fixture(name="pandas_frame")
def fixture_pandas_frame():

    """Define pandas dataframe to be used in tests"""
    indata = {"nums": [1, 2, 3], "letters": ["A", "B", "C"]}

    return pd.DataFrame(indata)


@pytest.fixture(name="arrow_table")
def fixture_arrow_table(pandas_frame):
    """Return pyarrow table from pandas dataframe
    args:
    frame (pd.DataFrame): the dataframe to convert
    returns: table (pa.Table): frame as pa.Table
    """
    schema = pa.Schema.from_pandas(pandas_frame)
    table = pa.Table.from_pandas(pandas_frame, schema=schema)
    return table


if __name__ == "__main__":
    print(fixture_agg_dummmy())
