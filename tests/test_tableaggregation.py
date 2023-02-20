"""Testing of classe TableAggregator"""
import logging
from sumo.table_aggregation import TableAggregator
from sumo.table_aggregation.utilities import timethis

logging.basicConfig(level="INFO", format="%(name)s %(levelname)s: %(message)s")

LOGGER = logging.getLogger()


@timethis("Whole process")
def test_table_aggregator():
    """Tests TableAggregator"""
    test_case_uuid = "270ac54e-dd42-4027-bd27-ccbb3bad9d3a"
    test_table_name = "summary"
    aggregator = TableAggregator(
        test_case_uuid,
        test_table_name,
        "eclipse",
        "iter-0",
        sumo_env="dev",
    )
    aggregator.aggregate()
    aggregator.upload()


if __name__ == "__main__":
    test_table_aggregator()
