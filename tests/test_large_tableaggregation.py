"""Testing of class TableAggregator"""
import logging
from sumo.table_aggregation import TableAggregator
from sumo.table_aggregation.utilities import timethis

logging.basicConfig(level="DEBUG", format="%(name)s %(levelname)s: %(message)s")

LOGGER = logging.getLogger()


@timethis("Whole process")
def test_table_aggregator():
    """Tests TableAggregator"""
    test_case_name = "7be66b47-4304-47d7-a949-0f7066cbcc1b"
    test_table_name = "DROGON"
    test_tag_name = "summary"
    aggregator = TableAggregator(
        test_case_name, test_table_name, test_tag_name, "iter-0"
    )
    aggregator.run()


if __name__ == "__main__":
    test_table_aggregator()
