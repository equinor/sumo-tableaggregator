"""Contains classes for aggregation of tables"""
from sumo.table_aggregation._utils import make_aggregated_frame


class TableAggregator:

    """Class for aggregating tables"""
    def __init__(self, case_name: str, table_name: str, **kwargs):
        """Reads the data to be aggregated
        args
        case_name (str): name of sumo case
        table_name (str): name of tables to aggregate
        """
        sumo_env = kwargs.get("sumo_env", "prod")
        self._object_ids = make_aggregated_frame(case_name, table_name, sumo_env)


    @property
    def object_ids(self):
        """Returns the _object_ids attribute"""
        return self._object_ids
