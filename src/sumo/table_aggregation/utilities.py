"""Utils for table aggregation"""
import logging
import warnings
import hashlib
import uuid
from typing import Dict, Union
from pathlib import Path
import yaml
import numpy as np
import pandas as pd
import pyarrow as pa
from pyarrow import feather
from sumo.wrapper import SumoClient


TMP = Path("tmp")


def init_logging(name: str) -> logging.Logger:
    """Inits a logging null handler
    args:
    name (str): name of logger
    returns logger (logging.Logger): the initialises logger
    """
    logger = logging.getLogger(name)
    logger.addHandler(logging.NullHandler())
    return logger


# The two functions below are stolen from fmu.dataio._utils
def md5sum(fname: str) -> str:
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


def write_yaml(write_dict: dict, filename: str):
    """Dump dictionary to yaml file
    args:
    write_dict (dict): dictionary to write
    filename (str): file to write to
    """
    with open(filename, "w", encoding="utf-8") as methandle:
        yaml.dump(write_dict, methandle)


def read_yaml(filename: str) -> dict:
    """Reads yaml file
    args:
    filename (str): file to write to
    returns yam (dict): results of the reading process
    """
    try:
        with open(filename, "r", encoding="utf-8") as methandle:
            yam = yaml.load(methandle, Loader=yaml.FullLoader)
    except IOError:
        warnings.warn(f"No file at {filename}")
    return yam


def uuid_from_string(string: str) -> str:
    """Produce valid and repeteable UUID4 as a hash of given string
    string (str): the string to make uuid from
    """
    return str(uuid.UUID(hashlib.md5(string.encode("utf-8")).hexdigest()))


# END of steal


def get_object(object_id: str, sumo: SumoClient, pandas: bool = True):
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


def arrow_to_frame(blob_object) -> pd.DataFrame:
    """Reads blob into pandas dataframe
    args:
    blob_object (dict): the object to read
    returns frame (pd.DataFrame): the extracted data
    """
    with pa.ipc.open_file(blob_object) as stream:
        frame = stream.read_pandas()
    return frame


def arrow_to_table(blob_object) -> pa.Table:
    """Reads sumo blob into pandas dataframe
    args:
    blob_object (dict): the object to read
    table (pa.Table): the read results
    """
    with pa.ipc.open_file(blob_object) as stream:
        table = stream.read_all()
    return table


def stat_frame_to_feather(frame: pd.DataFrame, agg_meta: dict, name: str):
    """Writes arrow format from pd.DataFrame with two multilevel index
    args:
    frame (pd.DataFrame): the data to write
    agg_meta (dict): Stub for aggregated meta to be written
    name (str): name of statistic
    """
    frame_cols = frame.columns
    for stat_type in frame_cols:

        agg_frame = pd.DataFrame(frame[stat_type])
        agg_frame.columns = [name]
        try:
            frame_cols = agg_frame.column_names
        except AttributeError:
            frame_cols = agg_frame.columns.tolist()

        # agg_meta["aggregation"] =  agg_meta.get("aggregation", {})
        agg_meta["fmu"]["aggregation"]["operation"] = stat_type
        write_feather(agg_frame, f"{name}_{stat_type}", agg_meta, frame_cols)


def decide_name(namer):
    """Gets name from list/pd.DataFrame.index or string
    args:
    namer (list, str, or pd.DataFrame.index): input for name
    returns name (str)
    """
    logger = init_logging(__name__ + ".decide_name")
    if isinstance(namer, str):
        name = namer
    else:
        try:
            namer = namer.tolist()
        except AttributeError:
            logger.warning("Input was not pd.DataFrame.columns")

        if len(namer) == 2:
            name = [col for col in namer if col != "REAL"].pop()
        else:
            if "BULK" in namer:
                name = "volumes"
            else:
                name = "summary"
            name = "aggregated_" + name

    logger.debug("Name of object will be: %s", name)
    return name


def frame_to_feather(frame: pd.DataFrame, agg_meta: dict, table_type: str = None):
    """Writes arrow format from pd.DataFrame
    args:
    frame (pd.DataFrame): the data to write
    agg_meta (dict): Stub for aggregated meta to be written
    table_type (str): name to be put in metadata, if None, autodetection
    """
    logger = init_logging(__name__ + ".frame_to_feather")
    frame_cols = frame.columns.tolist()
    if not TMP.exists():
        TMP.mkdir()

    if table_type is not None:
        namer = table_type
    else:
        namer = frame_cols
    name = decide_name(namer)
    logger.debug("Name: %s", name)
    write_feather(frame, name, agg_meta, frame_cols)


def write_feather(frame: pd.DataFrame, name: str, agg_meta: dict, columns: list):
    """Writes feather file
    args:
    frame (pd.DataFrame): the data to write
    agg_meta (dict): Stub for aggregated meta to be written
    columns (list): the column names in the frame
    """
    logger = init_logging(__name__ + ".write_feather")
    file_name = TMP / (name.lower() + ".arrow")
    meta_name = TMP / f".{file_name.name}.yml"

    agg_meta["data"]["spec"]["columns"] = columns
    agg_meta["data"]["name"] = name
    agg_meta["display"]["name"] = name
    agg_meta["file"]["absolute_path"] = str(file_name.absolute())
    agg_meta["file"]["relative_path"] = str(file_name)
    feather.write_feather(frame, dest=file_name)
    try:
        del agg_meta["_sumo"]
    except KeyError:
        logger.debug("Nothing to delete at _sumo")
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


class MetadataSet:

    """Class for arrangement of input to aggregation"""

    def __init__(self):
        """Sets _parameter_dict to empty dict"""
        self._parameter_dict = {}
        self._real_ids = set()
        self._uuids = set()

    @property
    def parameter_dict(self) -> dict:
        """Returns _parameter_dict attribute"""
        return self._parameter_dict

    @property
    def real_ids(self) -> tuple:
        """Returns _real_ids attribute"""
        return tuple(self._real_ids)

    @property
    def uuids(self) -> list:
        """Returns _uuid attribute"""
        return self._uuids

    def aggid(self) -> str:
        """Returns the hash of the sum of all the sorted(uuids)"""
        return str("".join(sorted(self.uuids)))

    def add_realisation(self, real_nr: int, real_parameters: dict):
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

    def base_meta(self, metadata: dict) -> dict:
        """Converts one metadata file into aggregated metadata
        args:
        metadata (dict): one valid metadatafile
        returns agg_metadata (dict): one valid metadata file to be used for
                                     aggregations to come
        """
        agg_metadata = convert_metadata(metadata, self.real_ids)
        return agg_metadata


def get_parent_id(result: dict) -> str:
    """Fetches parent id from one elastic search hit
    args:
    result (dict): one hit
    returns parent_id
    """
    parent_id = result["_source"]["_sumo"]["parent_object"]
    return parent_id


def split_results_and_meta(results: list) -> dict:
    """splits hits from sumo query
    results (list): query_results["hits"]["hist"]
    returns split_tup (tuple): tuple with split results
    """
    logger = init_logging(__name__ + ".split_result_and_meta")
    parent_id = get_parent_id(results[0])
    logger.debug(parent_id)
    meta = MetadataSet()
    blob_ids = {}
    for result in results:
        real_meta = result["_source"]
        try:
            real = real_meta["fmu"].pop("realization")
            name = real["id"]
        except KeyError:
            logger.warning("No realization in result, allready aggregation?")
        meta.add_realisation(name, real["parameters"])
        blob_ids[name] = result["_id"]
    agg_meta = meta.base_meta(real_meta)
    split_tup = (parent_id, blob_ids, agg_meta, meta.real_ids, meta.parameter_dict)
    return split_tup


def get_blob_ids_w_metadata(query_results: dict) -> dict:
    """splits query results
    get_results ()
    """
    logger = init_logging(__name__ + ".get_blob_ids_w_meta")
    total_count = query_results["hits"]["total"]["value"]

    logger.debug(total_count)
    hits = query_results["hits"]["hits"]
    logger.debug("hits: %s", len(hits))
    logger.debug(hits)
    return_count = len(hits)
    if return_count < total_count:
        message = (
            "Your query returned less than the total number of hits\n"
            + f"({return_count} vs {total_count}). You might wanna rerun \n"
            + f"the query with size set to {total_count}"
        )
        warnings.warn(message)
    return split_results_and_meta(hits)


def query_sumo(
    sumo: SumoClient, case_name: str, name: str, tag: str = "", content: str = "depth"
) -> tuple:
    """Fetches blob ids for relevant tables, collates metadata
    args:
    case_name (str): name of case
    name (str): name of table per realization
    tag (str): tagname for table
    content (str): table content
    sumo_env (str): what environment to communicate with
    """
    logger = init_logging(__name__ + ".query_sumo")
    query = (
        f"fmu.case.name:{case_name} AND data.name:{name} "
        + f"AND data.content:{content} AND class:table"
    )
    if tag:
        query += f" AND data.tagname:{tag}"
    logger.debug(query)
    query_results = sumo.get(path="/search", query=query, size=1000)
    return query_results


def query_for_tables(
    sumo: SumoClient, case_name: str, name: str, tag: str = "", content: str = "depth"
) -> tuple:
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


def aggregate_arrow(object_ids: Dict[str, str], sumo: SumoClient) -> pa.Table:
    """Aggregates the individual files into one large pyarrow table
    args:
    object_ids (dict): key is real nr, value is object id
    returns: aggregated (pa.Table): the aggregated results
    """
    aggregated = []
    for real_nr, object_id in object_ids.items():
        real_table = get_object(object_id, sumo, False)
        rows = real_table.shape[0]
        aggregated.append(real_table.add_column(0, "REAL", pa.array([real_nr] * rows)))
    aggregated = pa.concat_tables(aggregated)

    return aggregated


def aggregate_pandas(object_ids: Dict[str, str], sumo: SumoClient) -> pd.DataFrame:
    """Aggregates the individual files into one large pandas dataframe
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


def aggregate_objects(object_ids: Dict[str, str], sumo: SumoClient,
                      pandas: bool = True) -> Union[pd.DataFrame, pa.Table]:
    """Aggregates the individual files into one large object
    args:
    object_ids (dict): key is real nr, value is object id
    returns: aggregated (pd.DataFrame): the aggregated results
    """
    if pandas:
        aggregated = aggregate_pandas(object_ids, sumo)
    else:
        aggregated = aggregate_arrow(object_ids, sumo)
    return aggregated


def p10(array_like):
    """Returns p10 of array like
    args:
    array_like (array like): numpy array or pd.Series pd.DataFrame
    """
    return np.percentile(array_like, 90)


def p90(array_like):
    """Returns p90 of array like
    args:
    array_like (array like): numpy array or pd.Series pd.DataFrame
    """
    return np.percentile(array_like, 10)


def make_stat_aggregations(
    frame: pd.DataFrame, meta_stub, aggfuncs: list = ("mean", p10, p90)
):
    """Makes statistical aggregations from dataframe
    args
    frame (pd.DataFrame): data to process
    meta_stub (dict): dictionary that is start of creating proper metadata
    aggfuncs (list): what aggregations to process
    """
    logger = init_logging(__name__ + ".make_stat_aggregations")
    non_aggs = ["DATE", "REAL", "ENSEMBLE"]
    stat_curves = [name for name in frame.columns if name not in non_aggs]
    logger.info("Will do stats on these vectors %s ", stat_curves)
    logger.debug(stat_curves)
    for curve in stat_curves:
        stats = frame.groupby("DATE")[curve].agg(aggfuncs)
        logger.debug(stats)
        stat_frame_to_feather(stats, meta_stub, curve)
    return stats


def store_aggregated_objects(
    frame: pd.DataFrame,
    meta_stub: dict,
    keep_grand_aggregation: bool = True,
):
    """Stores results in temp folder
    frame (pd.DataFrame): the dataframe to store
    meta_stub (dict): dictionary that is start of creating proper metadata
    keep_grand_aggregation (bool): store copy of the aggregated or not
    withstats (bool): make statistical vectors as well
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


# def pyarrow_to_bytes(frame):
#     """Writing frame to bytestring directly
#     args:
#     """
#     sink = pa.BufferOutputStream()
#     with pa.ipc.new_stream(sink) as writer:
#
#         # writer = pa.ipc.new_stream(sink, batch.schema)
#
#         writer.write_batch(batch)


def meta_to_bytes(meta_path):
    """
    Given a path to a metadata file, find real fileread as bytes, return byte string.
    args:
    meta_path (str): name of metadatafile
    returns byte_string : file as bytes
    """
    # Basically stolen from sumo.wrapper
    path = meta_path.parent / meta_path.name[1:].replace(".yml", "")
    with open(path, "rb") as stream:
        byte_string = stream.read()

    return byte_string


def upload_aggregated(sumo: SumoClient, parent_id: str, store_dir: str = "tmp"):
    """Uploads files to sumo
    sumo (SumoClient): the client to use for uploading
    store_dir (str): name of folder where results are stored
    """
    logger = init_logging(__name__ + ".upload_aggregated")
    store = Path(store_dir)
    upload_files = list(store.glob("*"))
    logger.debug(upload_files)
    file_count = 0
    for upload_file in upload_files:
        if not upload_file.name.startswith("."):
            continue
        meta = read_yaml(upload_file)
        byte_string = meta_to_bytes(upload_file)
        path = f"/objects('{parent_id}')"
        response = sumo.post(path=path, json=meta)
        logger.debug(response.json())
        blob_url = response.json().get("blob_url")
        response = sumo.blob_client.upload_blob(blob=byte_string, url=blob_url)
        # return response
        file_count += 1
    logger.info("Uploaded %s files", file_count)
    return file_count
    # store.rmdir()


def convert_metadata(
    single_metadata: dict, real_ids: list, context="fmu", operation: str = "collection"
):
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
