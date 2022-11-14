"""Contains classes for aggregation of tables"""
from sumo.wrapper import SumoClient
from sumo.table_aggregation._utils import get_blob_ids_w_metadata


class TableAggregator:

    """Class for aggregating tables"""
    def __init__(self, case_name: str, table_name: str, **kwargs):
        """Reads the data to be aggregated
        args
        case_name (str): name of sumo case
        table_name (str): name of tables to aggregate
        """
        sumo_env = kwargs.get("sumo_env", "prod")
        self._sumo = SumoClient(sumo_env)
        self._object_ids, self._meta, self.p_meta = (
            get_blob_ids_w_metadata(case_name, table_name, self.sumo)
        )

    @property
    def sumo(self):
        """returns the _sumo_attribute"""
        return self._sumo

    @property
    def object_ids(self):
        """Returns the _object_ids attribute"""
        return self._object_ids
