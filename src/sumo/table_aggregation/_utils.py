"""Utils for table aggregation"""
import logging
import warnings
import pandas as pd
from sumo.wrapper import SumoClient


def init_logging(name):
    """Inits a logging null handler
    args:
    name (str): name of logger
    returns logger (logging.Logger): the initialises logger
    """
    logger = logging.getLogger(name)
    logger.addHandler(logging.NullHandler())
    return logger


class ParameterSet():

    """Class for arrangement of parameters during aggregation"""

    def __init__(self):
        """Sets _parameter_dict to empty dict"""
        self._parameter_dict = {}

    @property
    def parameter_dict(self):
        """Returns _parameter_dict attribute"""
        return self._parameter_dict

    def add_realisation(self, real_nr, real_parameters):
        """Adds parameters from one realisation
        args:
        real_parameters (dict): parameters from one realisation
        """
        for name in real_parameters:
            if name not in self._parameter_dict:
                self._parameter_dict[name] = {}
            self._parameter_dict[name][real_nr] = real_parameters[name]


def split_results_and_meta(results: list) -> dict:
    """splits hits from sumo query
    results (list): query_results["hits"]["hist"]
    returns split_tup (tuple): tuple with split results
    """
    meta = []
    parameter_meta = ParameterSet()
    blob_ids = {}
    for result in results:
        real_meta = result["_source"]
        meta.append(real_meta)
        real = real_meta["fmu"].pop("realization")
        name = str(real["id"])
        parameter_meta.add_realisation(name, real["parameters"])
        blob_ids[name] = result["_id"]
    split_tup = (blob_ids, meta, parameter_meta.parameter_dict)
    return split_tup


def split_results(query_results) -> dict:
    """splits query results
       get_results ()
    """
    total_count = query_results["hits"]["total"]["value"]

    logger = init_logging(__name__ + ".split_results_and_meta")

    print(total_count)
    hits = query_results["hits"]["hits"]
    print("hits: %s", len(hits))
    print(hits)
    return_count = len(hits)
    if return_count < total_count:
        message = (
            "Your query returned less than the total number of hits\n" +
            f"({return_count} vs {total_count}). You might wanna rerun \n" +
            f"the query with size set to {total_count}"
        )
        warnings.warn(message)
    return split_results_and_meta(hits)


def get_blob_ids_w_metadata(case_name: str, table_name: str, table_tag: str = "",
                            table_content: str = "depth", sumo_env: str = "prod") -> dict:
    """Fetches blob ids for relevant tables, collates metadata
    args:
    case_name (str): name of case
    table_name (str): name of table per realization
    table_tag (str): tagname for table
    table_content (str): table content
    sumo_env (str): what environment to communicate with
    """
    logger = init_logging(__name__ + ".get_blob_ids_w_metadata")
    sumo = SumoClient(sumo_env)
    query = (f"fmu.case.name:{case_name} AND data.name:{table_name} " +
             f"AND data.content:{table_content} AND class:table"
    )
    print(query)
    results = split_results(sumo.get(path="/search", query=query, size=1000))
    return results
