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
    for iter_name in ["iter-0", "iter-1"]:
        aggregator = TableAggregator(
            test_case_uuid,
            test_table_name,
            "eclipse",
            iter_name,
            "timeseries",
            sumo_env="dev",
            table_index=["DATE"],
        )
        aggregator.aggregate()
        aggregator.upload()


if __name__ == "__main__":
    test_table_aggregator()
