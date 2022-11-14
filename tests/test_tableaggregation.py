"""Testing of classe TableAggregator"""
import logging
from context import TableAggregator

logging.basicConfig(level="DEBUG",
                    format="%(name)s %(levelname)s: %(message)s")

LOGGER = logging.getLogger()


def test_table_aggregator():

    """Tests TableAggregator"""
    test_case_name = "drogon_design_2022_11-01"
    test_table_name = "summary"
    aggregated = TableAggregator(test_case_name, test_table_name)
    print(aggregated.object_ids)
