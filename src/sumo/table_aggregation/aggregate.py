"""Contains classes for aggregation of tables"""
import time
import pandas as pd
from sumo.wrapper import SumoClient
import sumo.table_aggregation.utilities as ut


class TableAggregator:

    """Class for aggregating tables"""

    def __init__(
        self, case_name: str, name: str, iteration: str, token: str = None, **kwargs
    ):
        """Reads the data to be aggregated
        args
        case_name (str): name of sumo case
        name (str): name of tables to aggregate
        token (str): authentication token
        """
        sumo_env = kwargs.get("sumo_env", "prod")
        self._delete = kwargs.get("delete", True)
        self._sumo = SumoClient(sumo_env, token)
        self._content = kwargs.get("content", "timeseries")
        self._case_name = case_name
        self._name = name
        self._iteration = iteration
        self._table_index = ["DATE"]
        # try:
        (
            self._parent_id,
            self._object_ids,
            self._meta,
            self._real_ids,
            self._p_meta,
        ) = ut.query_for_table(
            self.sumo,
            self._case_name,
            self._name,
            self._iteration,
            content=self._content,
        )

        # except Exception:
        # print("Something went wrong, dunno what!")

    @property
    def parent_id(self) -> str:
        """Returns _parent_id attribute"""
        return self._parent_id

    @property
    def table_index(self):
        """Return attribute _table_index

        Returns:
            string: the table index
        """
        return self._table_index

    @property
    def sumo(self) -> SumoClient:
        """returns the _sumo_attribute"""
        return self._sumo

    @property
    def object_ids(self) -> tuple:
        """Returns the _object_ids attribute"""
        return self._object_ids

    @property
    def iteration(self) -> str:
        """Returns the _iteration attribute"""
        return self._iteration

    @property
    def real_ids(self) -> list:
        """Returns _real_ids attribute"""
        return self._real_ids

    @property
    def parameters(self) -> dict:
        """Returns the _p_meta attribute"""
        return self._p_meta

    @property
    def base_meta(self) -> dict:
        """Returns _meta attribute"""
        return self._meta

    @property
    def aggregated(self) -> pd.DataFrame:
        """Returns the _aggregated attribute"""
        if self._aggregated is None:
            self.aggregate()

        return self._aggregated

    def aggregate(self):
        """Aggregates objects over realizations on disk
        args:
        redo (bool): shall self._aggregated be made regardless
        """
        start_time = time.perf_counter()
        self._aggregated = ut.aggregate_arrow(self.object_ids, self.sumo)
        end_time = time.perf_counter()
        print(f"Aggregated in {end_time - start_time} sec")

    def upload(self):
        """Uploads data to sumo"""
        # if self.aggregated is not None:

        #    ut.store_aggregated_objects(self.aggregated, self.base_meta)
        start_time = time.perf_counter()
        ut.extract_and_upload(
            self.sumo, self.parent_id, self.aggregated, self.table_index, self.base_meta
        )
        end_time = time.perf_counter()
        print(f"Uploaded in {end_time - start_time} sec")

    # def __del__(self):
    # """Deletes tmp folder"""
    # if self._delete:
    # try:
    # for single_file in self._tmp_folder.iterdir():
    # single_file.unlink()

    # self._tmp_folder.rmdir()
    # except FileNotFoundError:
    # print("No tmp folder exists, talk about failing fast :-)")
