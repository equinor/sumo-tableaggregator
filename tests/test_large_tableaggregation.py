"""Testing of classe TableAggregator"""
import logging
from sumo.table_aggregation import TableAggregator
from sumo.table_aggregation.utilities import timethis

logging.basicConfig(level="INFO", format="%(name)s %(levelname)s: %(message)s")

LOGGER = logging.getLogger()


@timethis("Whole process")
def test_table_aggregator():

    """Tests TableAggregator"""
    test_case_name = "snorre_sumo-g2_29-2022-12-08"
    test_table_name = "summary"
    aggregator = TableAggregator(
        test_case_name, test_table_name, 0, content="timeseries", sumo_env="dev"
    )
    aggregator.aggregate()
    aggregator.upload()


if __name__ == "__main__":
    test_table_aggregator()
