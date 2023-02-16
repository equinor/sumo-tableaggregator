"""Testing of classe TableAggregator"""
import os
import time
import logging
from sumo.table_aggregation import TableAggregator

logging.basicConfig(level="ERROR", format="%(name)s %(levelname)s: %(message)s")


def test_table_aggregator():
    """Tests TableAggregator"""
    test_case_uuid = "270ac54e-dd42-4027-bd27-ccbb3bad9d3a"
    test_table_name = "summary"
    print(f"Nr of cpus {os.cpu_count()}")
    start = time.perf_counter()
    aggregator = TableAggregator(
        test_case_uuid,
        test_table_name,
        0,
        tag="eclipse",
        content="timeseries",
        sumo_env="dev",
    )
    aggregator.aggregate()
    print(f"Aggregation took {time.perf_counter() - start: f4.2}")
    start = time.perf_counter()
    aggregator.upload()
    print(f"Upload took {time.perf_counter() - start: f4.2}")
    print("Goody")


if __name__ == "__main__":
    test_table_aggregator()
