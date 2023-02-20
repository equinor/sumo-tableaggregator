"""Testing of classe TableAggregator"""
import time
import logging
from sumo.table_aggregation import TableAggregator
from sumo.table_aggregation.utilities import timethis, get_object

logging.basicConfig(level="DEBUG", format="%(name)s %(levelname)s: %(message)s")

LOGGER = logging.getLogger()
LOGGER.setLevel("INFO")


# @pytest.fixture(name="table_aggregator")
def fixture_aggregator():
    """Init TableAggregator for case"""
    test_case_name = "drogon_design-2022-12-01"
    test_table_name = "summary"
    aggregator = TableAggregator(
        test_case_name, test_table_name, "", "iter-0", content="*"
    )
    return aggregator


@timethis("Whole process")
def test_table_aggregator(table_aggregator):

    """Tests TableAggregator"""

    table_aggregator.aggregate()
    table_aggregator.upload()
    return table_aggregator


def test_results(aggregator):
    """Tests the results"""
    time.sleep(1)
    # exit()
    result_query = aggregator.sumo.get(
        "/search",
        query=f"fmu.case.name:{aggregator.case_name} AND class:table AND fmu.aggregation:*",
        size=100,
    )
    hits = result_query["hits"]["hits"]
    correct_nr = 20
    print(f"Found  {len(hits)} aggregations")
    assert len(hits) == correct_nr
    for result in hits:
        meta = result["_source"]
        name = meta["data"]["name"]
        operation = meta["fmu"]["aggregation"]["operation"]
        columns = meta["data"]["spec"]["columns"]

        table = get_object(result["_id"], aggregator.sumo)
        print(f"{name}-{operation}: {table.column_names}")
        print(columns)


if __name__ == "__main__":
    test_results(test_table_aggregator(fixture_aggregator()))
