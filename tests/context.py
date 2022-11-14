import os
import sys
import sumo.wrapper
from pathlib import Path


# run the tests from the root dir
TEST_DIR = Path(__file__).parent / "../"
os.chdir(TEST_DIR)


def add_path():
    """Way to add package path to sys.path for testing"""
    # Adapted from https://docs.python-guide.org/writing/structure/
    # Turned into function because the details here didn't work
    package_path = str(Path(__file__).parent.absolute() / "../src/")
    while package_path in sys.path:
        sys.path.remove(package_path)
    sys.path.insert(0, package_path)

add_path()

from sumo.table_aggregation import TableAggregator
from sumo.table_aggregation import _utils as ut
