"""Testing of classe TableAggregator"""
import logging
from context import TableAggregator, Timer

logging.basicConfig(level="DEBUG", format="%(name)s %(levelname)s: %(message)s")

LOGGER = logging.getLogger()


def test_table_aggregator():

    """Tests TableAggregator"""
    test_case_name = "drogon_design-2022-12-01"
    test_table_name = "summary"
    aggregator = TableAggregator(test_case_name, test_table_name)
    timer = Timer()
    timer.start()
    print(aggregator.object_ids)
    print("Fetched ids")
    timer.stop()
    timer.start()
    aggregator.aggregate()
    timer.stop("Aggregated")
    print("Aggregated")
    timer.stop()
    timer.start()
    aggregator.write_statistics()
    print("Added statistics")
    timer.stop()
    timer.start()
    aggregator.upload()
    timer.stop()
    print("Goody")


if __name__ == "__main__":
    test_table_aggregator()
