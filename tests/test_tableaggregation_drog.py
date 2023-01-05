"""Testing of classe TableAggregator"""
import logging
from context import TableAggregator, Timer

logging.basicConfig(level="DEBUG", format="%(name)s %(levelname)s: %(message)s")

# LOGGER = logging.getLogger()


def test_table_aggregator():

    """Tests TableAggregator"""
    test_case_name = "drogon_ahm-2023-01-04"
    test_table_name = "summary"
    timer = Timer()
    timer.start()
    aggregator = TableAggregator(test_case_name, test_table_name, 0, delete=False, content="depth")
    print(f"Fetched ids: {timer.stop()}")
    # print(aggregator.object_ids)
    timer.start()
    aggregator.aggregate()
    print(f"Aggregated: {timer.stop(restart=True)}")
    # timer.start()
    # aggregator.write_statistics()
    # print(f"Added statistics {timer.stop(restart=True)}")
    # timer.start()
    aggregator.upload()
    print(f"Uploaded: {timer.stop()}")
    print("Goody")


if __name__ == "__main__":
    test_table_aggregator()
