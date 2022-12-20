"""Context for tests"""
import sys
import time
from pathlib import Path


# run the tests from the root dir
# TEST_DIR = Path(__file__).parent / "../"
# os.chdir(TEST_DIR)


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
from sumo.table_aggregation import utilities as ut

# Class and exception stolen from realpython
# https://realpython.com/python-timer/


class TimerError(Exception):
    """A custom exception used to report errors in use of Timer class"""


class Timer:

    """Class for timing the tests"""

    def __init__(self):
        self._start_time = None

    def start(self):
        """Start a new timer"""
        if self._start_time is not None:
            raise TimerError("Timer is running. Use .stop() to stop it")

        self._start_time = time.perf_counter()

    def stop(self, restart=False):
        """Stop the timer, and report the elapsed time"""
        if self._start_time is None:
            raise TimerError("Timer is not running. Use .start() to start it")

        elapsed_time = time.perf_counter() - self._start_time
        report = ""
        report += f"Elapsed time: {elapsed_time:0.4f} seconds"
        self._start_time = None
        if restart:
            self.start()
        return report
