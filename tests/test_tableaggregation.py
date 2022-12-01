"""Testing of classe TableAggregator"""
import logging
from context import TableAggregator, Timer

logging.basicConfig(level="DEBUG",
                    format="%(name)s %(levelname)s: %(message)s")

LOGGER = logging.getLogger()


def test_table_aggregator():

    """Tests TableAggregator"""
    test_case_name = "drogon_design-2022-12-01"
    test_table_name = "summary"
    aggregator = TableAggregator(test_case_name, test_table_name)
    timer = Timer()
    timer.start()
    print(aggregator.object_ids)
    timer.stop("Found object ids")
    aggregator.aggregate()
    timer.stop("Aggregated")
    aggregator.add_statistics()
    timer.stop("Added statistics")
    aggregator.upload()
    timer.stop("Uploaded")
    print("Goody")
