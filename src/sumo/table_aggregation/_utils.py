"""Utils for table aggregation"""
import logging
import warnings
import hashlib
import uuid
from typing import Dict
from pathlib import Path
import yaml
import pandas as pd
import pyarrow as pa
from pyarrow import feather
from sumo.wrapper import SumoClient


TMP = Path("tmp")


def init_logging(name):
    """Inits a logging null handler
    args:
    name (str): name of logger
    returns logger (logging.Logger): the initialises logger
    """
    logger = logging.getLogger(name)
    logger.addHandler(logging.NullHandler())
    return logger

# The two functions below are stolen from fmu.dataio._utils
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


def write_yaml(write_dict, filename):
    """Dump dictionary to yaml file
    args:
    write_dict (dict): dictionary to write
    filename (str): file to write to
    """
    with open(filename, "w", encoding="utf-8") as methandle:
        yaml.dump(write_dict, methandle)


def uuid_from_string(string):
    """Produce valid and repeteable UUID4 as a hash of given string
    string (str): the string to make uuid from
    """
    return str(uuid.UUID(hashlib.md5(string.encode("utf-8")).hexdigest()))

# END of steal


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
    blob_object (dict): the object to read
    returns frame (pd.DataFrame): the extracted data
    """
    with pa.ipc.open_file(blob_object) as stream:
        frame = stream.read_pandas()
    return frame


def arrow_to_table(blob_object):
    """Reads sumo blob into pandas dataframe
    args:
    blob_object (dict): the object to read
    table (pa.Table): the read results
    """
    with pa.ipc.open_file(blob_object) as stream:
        table = stream.read_all()
    return table


def frame_to_feather(frame, agg_meta, table_type=None):
    """Writes arrow format from pd.DataFrame
    args:
    frame (pd.DataFrame): the data to write
    agg_meta (dict): Stub for aggregated meta to be written
    table_type
    returns: table
    """
    if not TMP.exists():
        TMP.mkdir()
    frame_cols = frame.columns.tolist()
    if table_type is not None:
        name = table_type
    elif len(frame_cols) == 2:
        name = [col for col in frame_cols if col != "REAL"].pop()
    else:
        if "BULK" in frame_cols:
            name = "volumes"
        else:
            name = "summary"

        name = "aggregated_" + name
    file_name = TMP / (name + ".arrow")
    meta_name = TMP / f".{file_name.name}.yml"

    agg_meta["data"]["spec"]["columns"] = frame.columns.tolist()
    feather.write_feather(frame, dest=file_name)
    md5 = md5sum(file_name)
    agg_meta["file"]["checksum_md5"] = md5
    agg_meta["fmu"]["aggregation"]["id"] = uuid_from_string(md5)
    write_yaml(agg_meta, meta_name)


def get_blob(blob_id: str, sumo: SumoClient):
    """Fetches sumo blob
    args:
    blob_id (str): key is real name: value blob id
    returns blob (binary something):
    """
    query = f"/objects('{blob_id}')/blob"
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
        name = real["id"]
        meta.add_realisation(name, real["parameters"])
        blob_ids[name] = result["_id"]
    agg_meta = meta.base_meta(real_meta)
    split_tup = (blob_ids, agg_meta, meta.real_ids, meta.parameter_dict)
    return split_tup


def get_blob_ids_w_metadata(query_results: dict) -> dict:
    """splits query results
       get_results ()
    """
    total_count = query_results["hits"]["total"]["value"]

    logger = init_logging(__name__ + ".get_blob_ids_w_meta")

    logger.debug(total_count)
    hits = query_results["hits"]["hits"]
    logger.debug("hits: %s", len(hits))
    logger.debug(hits)
    return_count = len(hits)
    if return_count < total_count:
        message = (
            "Your query returned less than the total number of hits\n" +
            f"({return_count} vs {total_count}). You might wanna rerun \n" +
            f"the query with size set to {total_count}"
        )
        warnings.warn(message)
    return split_results_and_meta(hits)


def query_sumo(sumo: SumoClient, case_name: str, name: str,
                            tag: str = "", content: str = "depth") -> tuple:
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
    logger.debug(query)
    query_results = sumo.get(path="/search", query=query, size=1000)
    return query_results


def query_for_tables(sumo: SumoClient, case_name: str, name: str,
                            tag: str = "", content: str = "depth") -> tuple:
    """Fetches blob ids for relevant tables, collates metadata
    args:
    case_name (str): name of case
    name (str): name of table per realization
    tag (str): tagname for table
    content (str): table content
    sumo_env (str): what environment to communicate with
    """
    query_results = query_sumo(sumo, case_name, name, tag, content)
    results = get_blob_ids_w_metadata(query_results)
    return results


def aggregate_objects(object_ids: Dict[str, str], sumo: SumoClient):
    """Aggregates the individual files into one large pyarrow table
    args:
    object_ids (dict): key is real nr, value is object id
    returns: aggregated (pd.DataFrame): the aggregated results
    """
    aggregated = []
    for real_nr, object_id in object_ids.items():
        real_frame = get_object(object_id, sumo)
        real_frame["REAL"] = real_nr
        aggregated.append(real_frame)
    aggregated = pd.concat(aggregated)
    aggregated["REAL"] = aggregated["REAL"].astype(int)
    return aggregated


def store_aggregated_objects(frame: pd.DataFrame, meta_stub: dict,
                             keep_grand_aggregation: bool = True):
    """Stores results in temp folder
    frame (pd.DataFrame): the dataframe to store
    keep_grand_aggregation (bool): store copy of the aggregated or not
    """
    logger = init_logging(__name__ + ".store_aggregated_objects")
    count = 0
    if keep_grand_aggregation:
        frame_to_feather(frame, meta_stub)
        count += 1
    neccessaries = ["REAL"]
    unneccessaries = ["YEARS", "SECONDS", "ENSEMBLE"]
    for col_name in frame:
        if col_name in (neccessaries + unneccessaries):
            continue

        logger.info("Creation of file for %s", col_name)
        keep_cols = neccessaries + [col_name]
        export_frame = frame[keep_cols]
        frame_to_feather(export_frame, meta_stub)
        count += 1

    logger.info("%s files produced", count)


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
    logger = init_logging(__name__ + ".convert_metadata")
    agg_metadata = single_metadata.copy()
    # fmu.realization shall not be present
    try:
        del agg_metadata[context]["realization"]
    except KeyError:
        logger.debug("No realization part to delete")
    # Adding specific aggregation ones
    agg_metadata[context]["aggregation"] = agg_metadata[context].get("aggregation", {})
    agg_metadata[context]["aggregation"]["operation"] = operation
    agg_metadata[context]["aggregation"]["realization_ids"] = list(real_ids)
    # Since no file on disk, trying without paths
    agg_metadata["file"]["relative_path"] = ""
    agg_metadata["file"]["absolute_path"] = ""
    agg_metadata["data"]["spec"]["columns"] = []

    return agg_metadata
