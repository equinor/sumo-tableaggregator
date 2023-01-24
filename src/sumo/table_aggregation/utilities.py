"""Utils for table aggregation"""
import sys
import logging
import warnings
import hashlib
import uuid
from typing import Dict, Union
import asyncio
from concurrent.futures import ThreadPoolExecutor
import yaml
import numpy as np
import pyarrow as pa
from pyarrow import feather
import pyarrow.parquet as pq
from sumo.wrapper import SumoClient

# from adlfs import AzureBlobFileSystem


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
def md5sum(bytes_string: bytes) -> str:
    """Make checksum from bytestring
    args:
    bytes_string (bytes): byte string
    returns (str): checksum
    """
    hash_md5 = hashlib.md5()
    hash_md5.update(bytes_string)
    checksum = hash_md5.hexdigest()
    return checksum


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


def query_sumo_iterations(sumo: SumoClient, case_name: str):
    """Query for iterations connected to case
    args:
    case_name (str): name of case
    """
    select_id = "fmu.iteration.id"
    query = f"fmu.case.name:{case_name}"
    results = sumo.get(
        path="/search",
        query=query,
        size=1,
        select=select_id,
        buckets=select_id,
    )
    iterations = [
        bucket["key"] for bucket in results["aggregations"][select_id]["buckets"]
    ]
    return iterations


def query_sumo(
    sumo: SumoClient,
    case_name: str,
    name: str,
    iteration: str,
    tag: str = "",
    content: str = "timeseries",
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
        + f"AND data.content:{content} AND fmu.iteration.id:{iteration} AND class:table"
    )
    print(f" query: {query}")
    if tag:
        query += f" AND data.tagname:{tag}"
    logger.debug(query)
    logger.debug(query)
    query_results = sumo.get(path="/search", query=query, size=1000)
    return query_results


def query_for_table(
    sumo: SumoClient,
    case_name: str,
    name: str,
    iteration: str,
    tag: str = "",
    content: str = "timeseries",
) -> tuple:
    """Fetches blob ids for relevant tables, collates metadata
    args:
    case_name (str): name of case
    name (str): name of table per realization
    tag (str): tagname for table
    content (str): table content
    sumo_env (str): what environment to communicate with
    """
    query_results = query_sumo(sumo, case_name, name, iteration, tag, content)
    if query_results["hits"]["total"]["value"] == 0:
        raise RuntimeError("Query returned with no hits, if you want results: modify!")
    results = get_blob_ids_w_metadata(query_results)
    return results


def uuid_from_string(string: str) -> str:
    """Produce valid and repeteable UUID4 as a hash of given string
    string (str): the string to make uuid from
    """
    return str(uuid.UUID(hashlib.md5(string.encode("utf-8")).hexdigest()))


# END of steal


def get_object(object_id: str, sumo: SumoClient) -> pa.Table:
    """fetches sumo object as pa.Table

    Args:
        object_id (str): sumo object id
        sumo (SumoClient): client to a given environment

    Returns:
        table: the object as pyarrow
    """
    query = f"/objects('{object_id}')/blob"
    table = arrow_to_table(sumo.get(query))
    return table


def arrow_to_table(blob_object) -> pa.Table:
    """Reads sumo blob into pandas dataframe
    args:
    blob_object (dict): the object to read
    table (pa.Table): the read results
    """
    try:
        table = pq.read_table(pa.BufferReader(blob_object))
    except pa.lib.ArrowInvalid:
        table = feather.read_table(pa.BufferReader(blob_object))
    return table


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
            logger.warning("No realization in result, already aggregation?")
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


def aggregate_arrow(object_ids: Dict[str, str], sumo: SumoClient) -> pa.Table:
    """Aggregates the individual files into one large pyarrow table
    args:
    object_ids (dict): key is real nr, value is object id
    returns: aggregated (pa.Table): the aggregated results
    """
    aggregated = []
    for real_nr, object_id in object_ids.items():
        print(f"Real {real_nr}")
        real_table = get_object(object_id, sumo)
        rows = real_table.shape[0]
        aggregated.append(real_table.add_column(0, "REAL", pa.array([real_nr] * rows)))
    aggregated = pa.concat_tables(aggregated)

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
    table: pa.Table,
    vector: str,
    table_index: Union[list, str],
    aggfuncs: list = ("mean", "min", "max", p10, p90),
):
    """Make statistical aggregations from pyarrow dataframe
    args
    table (pa.Table): data to process
    meta_stub (dict): dictionary that is start of creating proper metadata
    aggfuncs (list): statistical aggregations to include
    logger  = init_logging(__name__ + ".table_to_bytes")st): what aggregations to process
    """
    logger = init_logging(__name__ + ".make_stat_aggregations")
    logger.info("Will do stats on vector %s ", vector)
    logger.debug("Stats on %s", vector)
    logger.debug(table_index)
    logger.debug(table.column_names)
    frame = table.to_pandas()
    stats = pa.Table.from_pandas(frame.groupby(table_index)[vector].agg(aggfuncs))
    keepers = [name for name in stats.column_names if name not in table_index]
    logger.debug(stats)
    return stats.select(keepers)


def prepare_object_launch(meta: dict, table, name, operation):
    """Complete metadata for object
    args:
    frame (pd.DataFrame): the data to write
    agg_meta (dict): Stub for aggregated meta to be written
    columns (list): the column names in the frame
    """
    logger = init_logging(__name__ + ".complete_meta")
    logger.debug("Converting %s", table)
    byte_string = table_to_bytes(table)
    unique_name = f"{name}--{operation}--{meta['fmu']['iteration']['name']}"
    md5 = md5sum(byte_string)
    logger.debug("Checksum %s", md5)
    meta["file"]["checksum_md5"] = md5
    meta["fmu"]["aggregation"]["id"] = uuid_from_string(md5)
    meta["file"]["checksum_md5"] = md5
    meta["fmu"]["aggregation"]["id"] = uuid_from_string(md5)
    meta["fmu"]["aggregation"]["operation"] = operation
    meta["data"]["spec"]["columns"] = table.column_names
    meta["data"]["name"] = name
    meta["display"]["name"] = name
    meta["file"]["relative_path"] = unique_name
    logger.debug("Metadata %s", meta)
    logger.debug(f"Object {unique_name} ready for launch")
    return byte_string, meta


def table_to_bytes(table: pa.Table):
    """Return table as bytestring

    Args:
        table (pa.Table): the table to be converted

    Returns:
        _type_: table as bytestring
    """
    logger = init_logging(__name__ + ".table_to_bytes")
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    byte_string = sink.getvalue().to_pybytes()
    logger.debug(type(byte_string))
    return byte_string


# def poster(func):
# response = "0"
# success_response = ("200", "201")
# while response not in success_response:
# try:
# response = func(*args)
# logger.debug("Response meta: %s", response.text)
# except Exception:
# exp_type, _, _ = sys.exc_info()
# logger.debug("Exception %s while uploading metadata", exp_type)

# def post_meta(parent_id: str, meta: dict):
# """posting metadata to sumo

# Args:
# parent_id (str): the parent to object
# meta (dict): the metadata
# """
# path = f"/objects('{parent_id}')"
# return sumo.post(path=path, json=meta)


# def post_blob(meta_response, byte_string: bytes):

# """posting metadata to sumo

# Args:
# parent_id (str): the parent to object
# meta (dict): the metadata
# """
# blob_url = meta_response.json().get("blob_url")
# return sumo.blob_client.upload_blob(blob=byte_string, url=blob_url)


async def call_parallel(loop, executor, func, *args):
    """Execute blocking function in an event loop"""
    # executor = ThreadPoolExecutor()
    executor = None
    return await loop.run_in_executor(executor, func, *args)


def upload_table(
    sumo: SumoClient, parent_id: str, table: pa.Table, name: str, meta: dict, operation
):
    """Upload single table

    Args:
        sumo (SumoClient): client with given environment
        parent_id (str): the parent id of the object
        table (pa.Table): the object to upload

    Returns:
        respons: The response of the object
    """
    logger = init_logging(__name__ + ".upload_table")
    logger.debug(parent_id)
    byte_string, meta = prepare_object_launch(meta, table, name, operation)
    path = f"/objects('{parent_id}')"
    rsp_nr = "0"
    success_response = (200, 201)
    while rsp_nr not in success_response:
        try:
            response = sumo.post(path=path, json=meta)
            rsp_nr = response.status_code
            logger.debug("response meta: %s", rsp_nr)
        except Exception:
            exp_type, _, _ = sys.exc_info()
            logger.debug("Exception %s while uploading metadata", exp_type)

    blob_url = response.json().get("blob_url")
    rsp_nr = "0"
    while rsp_nr not in success_response:
        try:
            response = sumo.blob_client.upload_blob(blob=byte_string, url=blob_url)
            rsp_nr = response.status_code
            logger.debug("Response blob %s", rsp_nr)
        except Exception:
            exp_type, _, _ = sys.exc_info()
            logger.debug("Exception %s while uploading metadata", exp_type)


async def upload_stats(
    sumo: SumoClient,
    parent_id: str,
    table: pa.Table,
    name: str,
    meta: dict,
    loop,
    executor,
):
    """Upload individual columns in table

    Args:
        sumo (SumoClient): client with given environment
        parent_id (str): the parent object id
        table (pa.Table): the table to split up
        name (str): name that will appear in sumo
        meta (dict): a metadata stub to be completed during upload
    """
    logger = init_logging(__name__ + ".upload_stats")
    logger.debug(table.column_names)
    for operation in table.column_names:
        logger.debug(operation)
        export_table = table.select([operation])
        # upload_table(sumo, parent_id, export_table, name, meta, aggtype=operation)
        await call_parallel(
            loop,
            executor,
            upload_table,
            sumo,
            parent_id,
            export_table,
            name,
            meta,
            operation,
        )


async def extract_and_upload(
    sumo: SumoClient,
    parent_id: str,
    table: pa.Table,
    table_index: list,
    meta_stub: dict,
    loop,
    executor,
    keep_grand_aggregation: bool = False,
):
    """Store results in temp folder
    table (pd.DataFrame): the dataframe to store
    meta_stub (dict): dictionary that is start of creating proper metadata
    keep_grand_aggregation (bool): store copy of the aggregated or not
    withstats (bool): make statistical vectors as well
    """
    logger = init_logging(__name__ + ".extract_and_upload")

    count = 0
    if keep_grand_aggregation:
        upload_table(sumo, parent_id, table, "FullyAggregated", meta_stub, "collection")
        count += 1
    neccessaries = ["REAL"] + table_index
    unneccessaries = ["YEARS", "SECONDS", "ENSEMBLE", "REAL"]
    for col_name in table.column_names:
        if col_name in (neccessaries + unneccessaries):
            continue
        # if not col_name.startswith("FOP"):
        # continue
        logger.debug("Working with %s", col_name)
        keep_cols = neccessaries + [col_name]
        logger.debug(keep_cols)
        export_frame = table.select(keep_cols)
        # upload_table(sumo, parent_id, export_frame, col_name, meta_stub)
        await call_parallel(
            loop,
            executor,
            upload_table,
            sumo,
            parent_id,
            export_frame,
            col_name,
            meta_stub,
            "collection",
        )
        # stats = make_stat_aggregations(export_frame, col_name, table_index)
        stats = await call_parallel(
            loop, executor, make_stat_aggregations, export_frame, col_name, table_index
        )
        logger.debug(stats)
        await upload_stats(sumo, parent_id, stats, col_name, meta_stub, loop, executor)
        # await call_parallel(
        # loop, upload_stats, sumo, parent_id, stats, col_name, meta_stub
        # )
        count += 1
    logger.info("%s files produced", count)


def convert_metadata(
    single_metadata: dict, real_ids: list, operation: str = "collection"
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
    try:
        del agg_metadata["_sumo"]
    except KeyError:
        logger.debug("Nothing to delete at _sumo")

    # fmu.realization shall not be present
    try:
        del agg_metadata["fmu"]["realization"]
    except KeyError:
        logger.debug("No realization part to delete")
    # Adding specific aggregation ones
    agg_metadata["fmu"]["aggregation"] = agg_metadata["fmu"].get("aggregation", {})
    agg_metadata["fmu"]["aggregation"]["operation"] = operation
    agg_metadata["fmu"]["aggregation"]["realization_ids"] = list(real_ids)
    agg_metadata["fmu"]["context"]["stage"] = "iteration"
    # Since no file on disk, trying without paths
    agg_metadata["file"]["absolute_path"] = ""
    agg_metadata["data"]["spec"]["columns"] = []

    return agg_metadata
