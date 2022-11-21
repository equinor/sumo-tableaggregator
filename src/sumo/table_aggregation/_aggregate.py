"""Contains classes for aggregation of tables"""
import pandas as pd
from sumo.wrapper import SumoClient
import sumo.table_aggregation._utils as ut


class TableAggregator:

    """Class for aggregating tables"""
    def __init__(self, case_name: str, name: str, **kwargs):
        """Reads the data to be aggregated
        args
        case_name (str): name of sumo case
        name (str): name of tables to aggregate
        """
        sumo_env = kwargs.get("sumo_env", "prod")
        self._sumo = SumoClient(sumo_env)
        self._aggregated = None
        self._object_ids, self._meta, self._real_ids, self._p_meta = (
            ut.query_for_tables(self.sumo, case_name, name)
        )

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
        """Returns the _p_meta attribute
        """
        return self._p_meta

    @property
    def base_meta(self) -> dict:
        """Returns _meta attribute"""
        return self._meta

    def aggregated(self, redo: bool = False) -> pd.DataFrame:
        """Aggregates objects over realizations on disk
        args:
        redo (bool): shall self._aggregated be made regardless
        """
        if redo or self._aggregated is None:
            self._aggregated = ut.aggregate_objects(self.object_ids, self.sumo)
        return self._aggregated

    def upload(self):
        """Uploads data to sumo
        """
        ut.store_aggregated_objects(self._aggregated, self.base_meta)
        ut.upload_aggregated(self.sumo, "tmp")
