"""Testing of classe TableAggregator"""
import os
import time
import logging
import asyncio
from sumo.table_aggregation import TableAggregator

logging.basicConfig(level="ERROR", format="%(name)s %(levelname)s: %(message)s")

# LOGGER = logging.getLogger()


def test_table_aggregator():

    """Tests TableAggregator"""
    test_case_name = "drogon_ahm-2023-01-04"
    test_table_name = "summary"
    start = time.perf_counter()
    # timer = Timer()
    # timer.start()
    print(f"Nr of cpus {os.cpu_count()}")
    aggregator = TableAggregator(
        test_case_name, test_table_name, 0, content="*", sumo_env="prod"
    )
    # print(f"Fetched ids: {timer.stop()}")
    # print(aggregator.object_ids)
    # timer.start()
    aggregator.aggregate()
    # print(f"Aggregated: {timer.stop(restart=True)}")
    # timer.start()
    # aggregator.write_statistics()
    # # print(f"Added statistics {timer.stop(restart=True)}")
    # timer.start()
    aggregator.upload()
    print(f"Took {time.perf_counter() - start}")
    print("Goody")


if __name__ == "__main__":
    test_table_aggregator()
