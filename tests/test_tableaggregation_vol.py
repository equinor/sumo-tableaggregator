"""Testing of classe TableAggregator"""
import logging
from sumo.table_aggregation import TableAggregator
from sumo.table_aggregation.utilities import timethis

logging.basicConfig(level="INFO", format="%(name)s %(levelname)s: %(message)s")

LOGGER = logging.getLogger()


@timethis("Whole process")
def test_table_aggregator():
    """Tests TableAggregator"""
    test_case_uuid = "10f41041-2c17-4374-a735-bb0de62e29dc"
    col_names = {
        # "summary": ["DATE"],
        "geogrid": ["ZONE", "REGION", "FACIES"],
        "simgrid": ["ZONE", "REGION"],
    }
    for test_table_name, cols in col_names.items():
        if test_table_name == "summary":
            content = "timeseries"
            tag = "eclipse"
        else:
            content = "volumes"
            tag = "vol"

        for iter_name in ["iter-0", "iter-1"]:
            print(f"{test_table_name}: {iter_name}")
            aggregator = TableAggregator(
                test_case_uuid,
                test_table_name,
                tag,
                iter_name,
                content,
                sumo_env="dev",
                table_index=cols,
            )
            aggregator.aggregate()
            aggregator.upload()


if __name__ == "__main__":
    test_table_aggregator()
