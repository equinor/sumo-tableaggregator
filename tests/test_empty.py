import logging
from sumo.table_aggregation import TableAggregator

logging.basicConfig(level="DEBUG", format="%(name)s %(levelname)s: %(message)s")

LOGGER = logging.getLogger()
LOGGER.setLevel("INFO")


def test_no_table_index(case_name):
    aggregator = TableAggregator(
        case_name, "random", "ecalc", "iter-0", content="timeseries"
    )
    aggregator.aggregate()
    aggregator.upload()
