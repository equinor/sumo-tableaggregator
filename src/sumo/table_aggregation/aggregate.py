"""Contains classes for aggregation of tables"""
import pandas as pd
from sumo.wrapper import SumoClient
import sumo.table_aggregation.utilities as ut


class TableAggregator:

    """Class for aggregating tables"""

    def __init__(self, case_name: str, name: str, token: str = None, **kwargs):
        """Reads the data to be aggregated
        args
        case_name (str): name of sumo case
        name (str): name of tables to aggregate
        token (str): authentication token
        """
        sumo_env = kwargs.get("sumo_env", "prod")
        self._sumo = SumoClient(sumo_env, token)
        self._tmp_folder = ut.TMP
        self._aggregated = None
        self._agg_stats = None
        (
            self._parent_id,
            self._object_ids,
            self._meta,
            self._real_ids,
            self._p_meta,
        ) = ut.query_for_tables(self.sumo, case_name, name)

    @property
    def parent_id(self) -> str:
        """Returns _parent_id attribute"""
        return self._parent_id

    @property
    def sumo(self) -> SumoClient:
        """returns the _sumo_attribute"""
        return self._sumo

    @property
    def object_ids(self) -> tuple:
        """Returns the _object_ids attribute"""
        return self._object_ids

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

    # @property
    # def aggregated_stats(self) -> pd.DataFrame:
    #     """Returns the _agg_stats attribute"""
    #     return self._agg_stats
    #
    def aggregate(self):
        """Aggregates objects over realizations on disk
        args:
        redo (bool): shall self._aggregated be made regardless
        """
        self._aggregated = ut.aggregate_objects(self.object_ids, self.sumo)
        self._aggregated.to_csv("Aggregated.csv", index=False)
        ut.store_aggregated_objects(self.aggregated, self.base_meta)

    def write_statistics(self):
        """Makes statistics from aggregated dataframe"""
        ut.make_stat_aggregations(self.aggregated, self.base_meta)

    def upload(self):
        """Uploads data to sumo"""
        if self.aggregated is not None:
            ut.store_aggregated_objects(self.aggregated, self.base_meta)
        ut.upload_aggregated(self.sumo, self.parent_id, self._tmp_folder)

    def __del__(self):
        """Deletes tmp folder"""
        for single_file in self._tmp_folder.iterdir():
            single_file.unlink()

        self._tmp_folder.rmdir()
