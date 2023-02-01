"""Testing of classe TableAggregator"""
import os
import time
import logging
from sumo.table_aggregation import TableAggregator

logging.basicConfig(level="ERROR", format="%(name)s %(levelname)s: %(message)s")


def test_table_aggregator():

    """Tests TableAggregator"""
    test_case_name = "drogon_ahm-2023-01-04"
    test_table_name = "summary"
    print(f"Nr of cpus {os.cpu_count()}")
    start = time.perf_counter()
    aggregator = TableAggregator(test_case_name, test_table_name, 0, content="*")
    aggregator.aggregate()
    print(f"Aggregation took {time.perf_counter() - start: f4.2}")
    start = time.perf_counter()
    aggregator.upload()
    print(f"Upload took {time.perf_counter() - start: f4.2}")
    print("Goody")


if __name__ == "__main__":
    test_table_aggregator()
