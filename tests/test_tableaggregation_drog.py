"""Testing of classe TableAggregator"""
import time
import logging
from sumo.table_aggregation import TableAggregator
from sumo.table_aggregation.utilities import timethis, get_object
import pytest

logging.basicConfig(level="INFO", format="%(name)s %(levelname)s: %(message)s")

LOGGER = logging.getLogger()


# @pytest.fixture(name="table_aggregator")
def fixture_aggregator():
    """Init TableAggregator for case"""
    test_case_name = "drogon_design-2022-12-01"
    test_table_name = "summary"
    aggregator = TableAggregator(test_case_name, test_table_name, 0, content="*")
    return aggregator


@timethis("Whole process")
def test_table_aggregator(table_aggregator):

    """Tests TableAggregator"""

    table_aggregator.aggregate()
    table_aggregator.upload()
    return table_aggregator


def test_results(aggregator):
    """Tests the results"""
    # time.sleep(10)
    result_query = aggregator.sumo.get(
        "/search",
        query=f"fmu.case.name:{aggregator.case_name} AND class:table AND fmu.aggregation:*",
        size=100,
    )
    hits = result_query["hits"]["hits"]
    print(f"Found  {len(hits)} aggregations")
    result_dict = {}
    for result in hits:
        name = result["_source"]["data"]["name"]
        operation = result["_source"]["fmu"]["aggregation"]["operation"]
        result_dict[f"{name}-{operation}"] = result["_id"]
    print(sorted(result_dict.keys()))
    for obj_name, object_id in result_dict.items():

        table = get_object(object_id, aggregator.sumo)
        print(f"{obj_name}: \n", table.column_names)


if __name__ == "__main__":
    test_results(test_table_aggregator(fixture_aggregator()))
