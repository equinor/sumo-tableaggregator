"""Utils for table aggregation"""
import logging
import warnings
import hashlib
import uuid
from typing import Dict
import pandas as pd
import pyarrow as pa
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

### The two functions below are stolen from fmu.dataio._utils
def md5sum(fname):
    """Makes checksum from file
    args:
    fname (str): name of file
    returns (str): checksum
    """
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as fil:
        for chunk in iter(lambda: fil.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def uuid_from_string(string):
    """Produce valid and repeteable UUID4 as a hash of given string
    string (str): the string to make uuid from
    """
    return str(uuid.UUID(hashlib.md5(string.encode("utf-8")).hexdigest()))

### END of steal

def get_object(object_id: str, sumo: SumoClient, pandas=True):
    """Gets object from blob store
    args:
    object_id (str): the id of the object
    sumo (SumoClient): the client to get from
    pandas (bool): defines what type to return
    returns digestable (pd.DataFrame) or pyarrow.Table)
    """
    if pandas:
        digestable = arrow_to_frame(get_blob(object_id, sumo))
    else:
        digestable = arrow_to_table(get_blob(object_id, sumo))
    return digestable


def arrow_to_frame(blob_object):
    """Reads blob into pandas dataframe
    args:
    blob_ids (dict): key is real name: value blob id
    real_nr (str): real nr
    returns frame (pd.DataFrame): the extracted data
    """
    with pa.ipc.open_file(blob_object) as stream:
        frame = stream.read_pandas()
    return frame


def arrow_to_table(blob_object):
    """Reads sumo blob into pandas dataframe
    args:
    blob_ids (dict): key is real name: value blob id
    real_nr (str): real nr
    returns frame (pd.DataFrame): the extracted data
    """
    with pa.ipc.open_file(blob_object) as stream:
        table = stream.read_all()
    return table


def get_blob(blob_id: str, sumo: SumoClient, table=True):
    """Fetches sumo blob
    args:
    blob_id (str): key is real name: value blob id
    returns blob (binary something):
    """
    query =  f"/objects('{blob_id}')/blob"
    blob = sumo.get(query)
    return blob


class MetadataSet():

    """Class for arrangement of input to aggregation"""

    def __init__(self):
        """Sets _parameter_dict to empty dict"""
        self._parameter_dict = {}
        self._real_ids = set()
        self._uuids = set()

    @property
    def parameter_dict(self):
        """Returns _parameter_dict attribute"""
        return self._parameter_dict

    @property
    def real_ids(self):
        """Returns _real_ids attribute"""
        return tuple(self._real_ids)

    @property
    def uuids(self):
        """Returns _uuid attribute"""
        return self._uuids

    def aggid(self):
        """Returns the hash of the sum of all the sorted(uuids)"""
        return str("".join(sorted(self.uuids)))

    def add_realisation(self, real_nr, real_parameters):
        """Adds parameters from one realisation
        args:
        real_parameters (dict): parameters from one realisation
        """
        self._real_ids.add(real_nr)
        # self._uiids.add(
        for name in real_parameters:
            if name not in self._parameter_dict:
                self._parameter_dict[name] = {}
            self._parameter_dict[name][real_nr] = real_parameters[name]

    def base_meta(self, metadata):
        """Converts one metadata file into aggregated metadata
        args:
        metadata (dict): one valid metadatafile
        returns agg_metadata (dict): one valid metadata file to be used for
                                     aggregations to come
        """
        agg_metadata = convert_metadata(metadata, self.real_ids)
        return agg_metadata


def split_results_and_meta(results: list) -> dict:
    """splits hits from sumo query
    results (list): query_results["hits"]["hist"]
    returns split_tup (tuple): tuple with split results
    """
    meta = MetadataSet()
    blob_ids = {}
    for result in results:
        real_meta = result["_source"]
        real = real_meta["fmu"].pop("realization")
        name = str(real["id"])
        meta.add_realisation(name, real["parameters"])
        blob_ids[name] = result["_id"]
    agg_meta = meta.base_meta(real_meta)
    split_tup = (blob_ids, agg_meta, meta.real_ids, meta.parameter_dict)
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


def get_blob_ids_w_metadata(sumo: SumoClient, case_name: str, name: str,
                            tag: str = "", content: str = "depth") -> dict:
    """Fetches blob ids for relevant tables, collates metadata
    args:
    case_name (str): name of case
    name (str): name of table per realization
    tag (str): tagname for table
    content (str): table content
    sumo_env (str): what environment to communicate with
    """
    logger = init_logging(__name__ + ".get_blob_ids_w_metadata")
    query = (f"fmu.case.name:{case_name} AND data.name:{name} " +
             f"AND data.content:{content} AND class:table"
    )
    if tag:
        query += f" AND data.tagname:{tag}"
    print(query)
    results = split_results(sumo.get(path="/search", query=query, size=1000))
    return results


def aggregate_objects(object_ids: Dict[str, str], sumo: SumoClient):
    """Aggregates the individual files into one large pyarrow table
    args:
    object_ids (dict): key is real nr, value is object id
    returns: aggregated (pd.DataFrame): the aggregated results
    """
    aggregated = []
    for real_nr, object_id in object_ids.items():
        real_frame =  get_object(blob_id, sumo)
        real_frame["REAL"] = real_nr
        aggregated.append(real_frame)
    aggregated = pd.concat(aggregated)
    return aggregated


def convert_metadata(single_metadata, real_ids, context="fmu", operation="collection"):
    """Makes metadata for the aggregated data from single metadata
    args:
    single_metadata (dict): one single metadata dict
    real_ids (list): list of realization numbers, needed for metadata
    context (str): the context that this comes from, currently the only
                   existing is fmu
    operation (str): what type of operation the aggregation performs
    returns agg_metadata (dict): metadata dict that can be further used for aggregation
    """
    agg_metadata = single_metadata.copy()
    # fmu.realization shall not be present
    try:
        del agg_metadata[context]["realization"]
    except KeyError:
        print("There you go!")
    # Adding specific aggregation ones
    agg_metadata[context]["aggregation"] = agg_metadata[context].get("aggregation", {})
    agg_metadata[context]["aggregation"]["operation"] = operation
    agg_metadata[context]["aggregation"]["realization_ids"] = real_ids
    # Since no file on disk, trying without paths
    agg_metadata["file"]["relative_path"] = ""
    agg_metadata["file"]["absolute_path"] = ""
    agg_metadata["data"]["spec"]["columns"] = []

    return agg_metadata
