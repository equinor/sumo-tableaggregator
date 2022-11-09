"""Utils for table aggregation"""
import pandas as pd
from fmu.sumo.explorer import Explorer
from fmu.sumo.explorer._utils import init_logging


def make_aggregated_frame(case_name: str, table_name: str,
                          sumo_env="prod") -> pd.DataFrame:
    """Aggregates data from sumo
    args:
    case_name (str): name of case in sumo
    sumo_env (str): name of sumo environment to aggregate from
    """
    logger = init_logging(__name__ + ".make_aggregated_frame")
    case = Explorer(sumo_env).get_case_by_name(case_name)
    object_ids = case.get_blob_ids(name=table_name, tag="", data_type="table")
    print(object_ids)
