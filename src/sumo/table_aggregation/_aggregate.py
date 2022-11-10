"""Contains classes for aggregation of tables"""
import logging
from sumo.table_aggregation._utils import get_blob_ids_w_metadata


logging.basicConfig(level="DEBUG",
                    format="%(name)s %(levelname)s: %(message)s")

LOGGER = logging.getLogger()


class TableAggregator:

    """Class for aggregating tables"""
    def __init__(self, case_name: str, table_name: str, **kwargs):
        """Reads the data to be aggregated
        args
        case_name (str): name of sumo case
        table_name (str): name of tables to aggregate
        """
        sumo_env = kwargs.get("sumo_env", "prod")
        self._object_ids, self._meta, self.p_meta = (
            get_blob_ids_w_metadata(case_name, table_name, sumo_env=sumo_env)
        )

    @property
    def object_ids(self):
        """Returns the _object_ids attribute"""
        return self._object_ids
