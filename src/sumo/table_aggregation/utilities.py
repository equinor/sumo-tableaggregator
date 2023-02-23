"""Utils for table aggregation"""
import os
import sys
import time
import logging
import warnings
import hashlib
import uuid
from typing import Dict, Union
import asyncio
from multiprocessing import get_context
from copy import deepcopy
import numpy as np
import pandas as pd
import pyarrow as pa
from pyarrow import feather
import pyarrow.parquet as pq
from sumo.wrapper import SumoClient
from sumo.wrapper._request_error import PermanentError
from io import BytesIO


def timethis(label):
    """Decorate functions to time them

    Args:
        label (str): name to shown when decorating
    """

    def decorator(func):
        logger = init_logging(__name__ + ".timer")

        def wrapper(*args, **kwargs):
            start = time.perf_counter()
            result = func(*args, **kwargs)
            stop = time.perf_counter()
            logger.info("--> Timex (%s): %s s", label, round(stop - start, 2))
            return result

        return wrapper

    return decorator


def init_logging(name: str) -> logging.Logger:
    """Init logging null handler
    args:
    name (str): name of logger
    returns (logging.Logger): an initialized logger
    """
    logger = logging.getLogger(name)
    logger.addHandler(logging.NullHandler())
    return logger


def md5sum(bytes_string: bytes) -> str:
    """Make checksum from bytestring
    args:
    bytes_string (bytes): byte string
    returns (str): checksum
    """
    logger = init_logging(__name__ + ".md5sum")
    hash_md5 = hashlib.md5()
    hash_md5.update(bytes_string)
    checksum = hash_md5.hexdigest()
    logger.debug("Checksum %s", checksum)

    return checksum


def return_uuid(sumo, identifier, version=4):
    """Checks if uuid has correct structure
    args:
    identifier (str): either case name of case uuid (prefered)
    version (int): what version of uuid to compare to
    """
    # Concepts stolen from stackoverflow.com
    # questions/19989481/how-to-determine-if-a-string-is-a-valid-v4-uuid
    logger = init_logging(__name__ + ".return_uuid")
    logger.info("Checking %s", identifier)
    try:
        logger.info("Checking for uuid")
        uuid.UUID(identifier, version=version)
    except ValueError:
        logger.info("%s should be the name of a case", identifier)
        warnings.warn(
            "Using case name: this is not the prefered option,"
            "might in the case of duplicate case names give errors"
        )
        logger.info("Passing %s to return a uuid", identifier)
        identifier = query_for_sumo_id(sumo, identifier)
        logger.info("After query we are left with %s", identifier)
    return identifier


def query_for_sumo_id(sumo: SumoClient, case_name: str) -> str:
    """Find uuid for given case name

    Args:
        sumo (SumoClient): initialized sumo client
        case_name (str): name of case

    Returns:
        str: case uuid
    """
    select = "fmu.case.uuid"
    query = f"fmu.case.name:{case_name}"
    results = sumo.get(
        path="/searchroot",
        query=query,
        size=1,
        select=select,
    )
    print(results["hits"]["hits"])
    unique_id = results["hits"]["hits"][0]["_source"]["fmu"]["case"]["uuid"]
    return unique_id


def query_sumo_iterations(sumo: SumoClient, case_uuid: str) -> list:
    """Qeury for iteration names

    Args:
        sumo (SumoClient): initialized sumo client
        case_uuid (str): name of case

    Returns:
        list: list with iteration numbers
    """
    logger = init_logging(__name__ + ".query_sumo_iterations")
    query = f"fmu.case.uuid:{case_uuid}"
    logger.info(query)
    selector = "fmu.iteration.name"
    bucket_name = selector + ".keyword"
    results = sumo.get(
        path="/search",
        query=query,
        size=1,
        select=selector,
        buckets=bucket_name,
    )
    iterations = [
        bucket["key"] for bucket in results["aggregations"][bucket_name]["buckets"]
    ]
    return iterations


def query_sumo(
    sumo: SumoClient,
    case_uuid: str,
    name: str,
    tag: str,
    iteration: str,
    content: str,
) -> dict:
    """Query for given table type

    Args:
        sumo (SumoClient): initialized sumo client
        case_uuid (str): case uuid
        name (str): name of table
        iteration (str): iteration number
        tag (str, optional): tagname of table. Defaults to "".
        content (str): content of table

    Returns:
        dict: query results
    """
    logger = init_logging(__name__ + ".query_sumo")
    logger.debug(
        "At query: id: %s, name: %s, tag: %s, it: %s, content: %s",
        case_uuid,
        name,
        tag,
        iteration,
        content,
    )
    query = (
        f"fmu.case.uuid:{case_uuid} AND data.name:{name} AND data.tagname:{tag} "
        + f"AND data.content:{content} AND fmu.iteration.name:'{iteration}' AND class:table AND NOT fmu.aggregation.operation:*"
    )
    logger.info("This is the query %s \n", query)
    query_results = sumo.get(path="/search", query=query, size=1000)
    return query_results


def query_for_table(
    sumo: SumoClient,
    case_uuid: str,
    name: str,
    tag: str,
    iteration: str,
    content: str,
    **kwargs: dict,
) -> tuple:
    """Fetch object id numbers and metadata

    Args:
        sumo (SumoClient): intialized sumo client
        case_uuid (str): case uuid
        name (str): name of table
        tag (str, optional): tagname of table. Defaults to "".
        iteration (str): iteration number
        content (str, optional): content of table. Defaults to "timeseries".

    Raises:
        RuntimeError: if no tables found

    Returns:
        tuple: contains parent id, object ids, meta data stub, all real numbers
               and dictionary containing all global variables for all realizations
    """
    logger = init_logging(__name__ + ".query_for_table")
    logger.debug(
        "Passing to query: id: %s, name: %s, tag: %s, it: %s, content: %s",
        case_uuid,
        name,
        tag,
        iteration,
        content,
    )
    query_results = query_sumo(sumo, case_uuid, name, tag, iteration, content)
    if query_results["hits"]["total"]["value"] == 0:
        raise RuntimeError("Query returned with no hits, if you want results: modify!")
    results = get_blob_ids_w_metadata(query_results, **kwargs)
    return results


def uuid_from_string(string: str) -> str:
    """Generate uuid from string

    Args:
        string (str): string to generate from

    Returns:
        str: uuid which is hash of md5
    """
    return str(uuid.UUID(hashlib.md5(string.encode("utf-8")).hexdigest()))


def get_object(object_id: str, sumo: SumoClient) -> pa.Table:
    """fetche sumo object as pa.Table

    Args:
        object_id (str): sumo object id
        sumo (SumoClient): client to a given environment

    Returns:
        pa.Table: the object as pyarrow
    """
    query = f"/objects('{object_id}')/blob"
    try:
        table = arrow_to_table(sumo.get(query))
    except (PermanentError, ConnectionError):
        time.sleep(0.5)
        table = get_object(object_id, sumo)

    return table


def arrow_to_table(blob_object) -> pa.Table:
    """Reads sumo blob into pandas dataframe
    args:
    blob_object (dict): the object to read
    pa.Table: the read results
    """
    logger = init_logging(__name__ + ".arrow_to_table")

    try:
        table = pq.read_table(pa.BufferReader(blob_object))
    except pa.lib.ArrowInvalid:
        try:
            table = feather.read_table(pa.BufferReader(blob_object))
        except pa.lib.ArrowInvalid:
            table = pa.Table.from_pandas(pd.read_csv(BytesIO(blob_object)))
    logger.debug("Returning table with columns %s", table.column_names)
    return table


class MetadataSet:

    """Class for arrangement of input to aggregation"""

    def __init__(self, table_index=None):
        """Sets _parameter_dict to empty dict"""
        self._parameter_dict = {}
        self._real_ids = set()
        self._uuids = set()
        self._table_index = table_index

    @property
    def parameter_dict(self) -> dict:
        """Return _parameter_dict attribute"""
        return self._parameter_dict

    @property
    def real_ids(self) -> tuple:
        """Return _real_ids attribute"""
        return tuple(self._real_ids)

    @property
    def uuids(self) -> list:
        """Return _uuid attribute"""
        return self._uuids

    @property
    def table_index(self):
        """Return attribute _table_index

        Returns:
            list: the table index
        """
        return self._table_index

    def aggid(self) -> str:
        """Return the hash of the sum of all the sorted(uuids)"""
        return str("".join(sorted(self.uuids)))

    def add_realisation(self, real_nr: int, real_parameters: dict):
        """Adds parameters from one realisation
        args:
        real_parameters (dict): parameters from one realisation
        """
        self._real_ids.add(real_nr)
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
        agg_metadata = convert_metadata(metadata, self.real_ids, self.table_index)
        self._table_index = agg_metadata["data"]["table_index"]
        return agg_metadata


def get_parent_id(result: dict) -> str:
    """Fetch parent id from one elastic search hit
    args:
    result (dict): one hit
    returns parent_id
    """
    parent_id = result["_source"]["_sumo"]["parent_object"]
    return parent_id


def split_results_and_meta(results: list, **kwargs: dict) -> tuple:
    """split hits from sumo query
    results (list): query_results["hits"]["hist"]
    returns tuple: tuple with parent id, object ids, meta stub, all real numbers
                   and global variables dict for all realizations
    """
    logger = init_logging(__name__ + ".split_result_and_meta")
    parent_id = get_parent_id(results[0])
    logger.debug("Parent id %s", parent_id)
    col_set = set()
    meta = MetadataSet(kwargs.get("table_index", None))
    blob_ids = {}
    for result in results:
        real_meta = result["_source"]
        col_set.add(len(real_meta["data"]["spec"]["columns"]))
        try:
            real = real_meta["fmu"].pop("realization")
            name = real["id"]
        except KeyError:
            logger.warning("No realization in result, already aggregation?")
        try:
            meta.add_realisation(name, real["parameters"])
        except KeyError:
            meta.add_realisation(name, [])
            logger.warning("There is no parameter key in meta")

        blob_ids[name] = result["_id"]
    if len(col_set) != 1:
        raise ValueError(
            "Whooa! Something severly wrong: nr of columns varies\n"
            "between individual realisations over your iteration\n"
            "This must be fixed before table aggregation is possible"
        )

    agg_meta = meta.base_meta(real_meta)
    split_tup = (
        parent_id,
        blob_ids,
        agg_meta,
        meta.real_ids,
        meta.parameter_dict,
        meta.table_index,
    )
    return split_tup


def get_blob_ids_w_metadata(query_results: dict, **kwargs: dict) -> tuple:
    """Get all object ids and metadata for iteration

    Args:
        query_results (dict): results from sumo query

    Returns:
        tuple: see under split results_and_meta
    """
    logger = init_logging(__name__ + ".get_blob_ids_w_meta")
    total_count = query_results["hits"]["total"]["value"]

    logger.debug(" Total number of hits existing %s", total_count)
    hits = query_results["hits"]["hits"]
    logger.debug("hits actually contained in request: %s", len(hits))
    return_count = len(hits)
    if return_count < total_count:
        message = (
            "Your query returned less than the total number of hits\n"
            + f"({return_count} vs {total_count}). You might wanna rerun \n"
            + f"the query with size set to {total_count}"
        )
        warnings.warn(message)
    return split_results_and_meta(hits, **kwargs)


def reconstruct_table(object_id: str, real_nr: str, sumo: SumoClient) -> pa.Table:
    """Reconstruct pa.Table from sumo object id

    Args:
        object_id (str): the object to fetch
        real_nr (str): the real nr of the object
        sumo (SumoClient): initialized sumo client

    Returns:
        pa.Table: The table
    """
    logger = init_logging(__name__ + ".reconstruct_table")
    logger.debug("Real %s", real_nr)
    real_table = get_object(object_id, sumo)
    rows = real_table.shape[0]
    real_table = real_table.add_column(0, "REAL", pa.array([np.int16(real_nr)] * rows))
    logger.debug("Table created %s", real_table)
    return real_table


async def aggregate_arrow(
    object_ids: Dict[str, str], sumo: SumoClient, loop
) -> pa.Table:
    """Aggregate the individual objects into one large pyarrow table
    args:
    object_ids (dict): key is real nr, value is object id
    sumo (SumoClient): initialized sumo client
    loop (asyncio.event_loop)
    returns: pa.Table: the aggregated results
    """
    aggregated = []
    for real_nr, object_id in object_ids.items():
        aggregated.append(
            call_parallel(loop, None, reconstruct_table, object_id, real_nr, sumo)
        )
    aggregated = pa.concat_tables(await asyncio.gather(*aggregated))
    return aggregated


def p10(array_like: Union[np.array, pd.DataFrame]) -> np.array:
    """Return p10 of array like
    args:
    array_like (array like): numpy array or pd.Series pd.DataFrame
    """
    return np.percentile(array_like, 90)


def p90(array_like: Union[np.array, pd.DataFrame]) -> np.array:
    """Return p90 of array like
    args:
    array_like (array like): numpy array or pd.Series pd.DataFrame
    """
    return np.percentile(array_like, 10)


def do_stats(frame, index, col_name, aggfunc, aggname):
    """Make single stat from table

    Args:
        frame (pd.DataFrame): the table to interrogate
        index (list): what to group over
        vector (str): the column to make stat on
        aggfuncs (str): the statistical operation

    Returns:
        pa.Table: the static
    """
    logger = init_logging(__name__ + ".do_stats")
    # frame = table.to_pandas()
    logger.debug("Columns prior to groupby: %s", frame.columns)
    stat = frame.groupby(index)[col_name].agg(aggfunc).to_frame().reset_index()
    table = pa.Table.from_pandas(stat)
    output = (aggname, table)
    logger.debug("%s %s", output[0], output[1].column_names)
    return output


@timethis("multiprocessing")
def make_stat_aggregations(
    table_dict: dict,
    table_index: Union[list, str],
    # aggfuncs: list = ("mean", "min", "max", p10, p90),
):
    """Make statistical aggregations from pyarrow dataframe
    args
    table (pa.Table): data to process
    meta_stub (dict): dictionary that is start of creating proper metadata
    aggfuncs (list): statistical aggregations to include
    logger  = init_logging(__name__ + ".table_to_bytes")st): what aggregations to process
    """
    logger = init_logging(__name__ + ".make_stat_aggregations")
    logger.info("Running with %s cpus", os.cpu_count())
    aggdict = {"mean": "mean", "min": "min", "max": "max", "p10": p10, "p90": p90}
    stat_input = []
    for col_name, table in table_dict.items():

        logger.debug("Calculating statistics on vector %s", col_name)
        logger.debug("Table index %s", table_index)
        logger.debug(
            "Columns before conversion to pandas df %s (size %s)",
            table.column_names,
            table.shape,
        )
        logger.debug(table.schema)
        frame = deepcopy(
            table.to_pandas(
                ignore_metadata=True,
            )
        )
        logger.debug(
            "Columns after conversion to pandas df %s (size %s)",
            frame.columns,
            frame.shape,
        )
        stat_input.extend(
            [
                (frame, table_index, col_name, aggfunc, aggname)
                for aggname, aggfunc in aggdict.items()
            ]
        )
    # for s_in in stat_input:
    #     print(s_in)
    # exit()
    logger.info("Submitting %s tasks to multprocessing", len(stat_input))

    stats = pa.Table.from_arrays(pa.array([]))
    with get_context("spawn").Pool() as pool:
        stats = pool.starmap(do_stats, stat_input)
    return stats


def prepare_object_launch(meta: dict, table, name, operation):
    """Complete metadata for object
    args:
    frame (pd.DataFrame): the data to write
    agg_meta (dict): Stub for aggregated meta to be written
    columns (list): the column names in the frame
    """
    logger = init_logging(__name__ + ".complete_meta")
    logger.debug("Preparing with data source %s", table)
    byte_string = table_to_bytes(table)
    tag = meta["data"]["tagname"]
    md5 = md5sum(byte_string)
    full_meta = deepcopy(meta)
    parent = full_meta["data"]["name"]
    logger.info("Name prior to change: %s", parent)
    unique_name = (
        parent
        + f"--{name}--{tag}--{operation}--"
        + full_meta["fmu"]["iteration"]["name"]
    )
    full_meta["file"]["checksum_md5"] = md5
    full_meta["fmu"]["aggregation"]["id"] = uuid_from_string(md5)
    full_meta["file"]["checksum_md5"] = md5
    full_meta["fmu"]["aggregation"]["id"] = uuid_from_string(md5)
    full_meta["fmu"]["aggregation"]["operation"] = operation

    if name == "table_index":
        full_meta["data"]["content"] = "table_index"
    # if full_meta["data"]["table_index"] == full_meta["data"]["spec"]["columns"]:
    # unique_name = "index--" + unique_name
    full_meta["data"]["spec"]["columns"] = table.column_names
    if operation == "collection":
        full_meta["data"]["table_index"].append("REAL")
    # full_meta["data"]["name"] = name
    full_meta["display"]["name"] = name
    full_meta["file"]["relative_path"] = unique_name
    logger.debug("Metadata %s", full_meta)
    logger.info("Object %s ready for launch", unique_name)
    return byte_string, full_meta


def table_to_bytes(table: pa.Table):
    """Return table as bytestring

    Args:
        table (pa.Table): the table to be converted

    Returns:
        _type_: table as bytestring
    """
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    byte_string = sink.getvalue().to_pybytes()
    return byte_string


async def call_parallel(loop, executor, func, *args):
    """Execute blocking function in an event loop"""
    return await loop.run_in_executor(executor, func, *args)


def upload_table(
    sumo: SumoClient, parent_id: str, table: pa.Table, name: str, meta: dict, operation
):
    """Upload single table

    Args:
        sumo (SumoClient): client with given environment
        parent_id (str): the parent id of the object
        table (pa.Table): the object to upload
        name (str): name to fill the data.name tag
        meta (dict): meta stub to pass on to completion of metadata
        operation (str): operation type

    """
    logger = init_logging(__name__ + ".upload_table")
    logger.debug("Uploading %s-%s", name, operation)
    logger.debug("Columns in table %s", table.column_names)
    logger.debug("Uploading to parent with id %s", parent_id)
    byte_string, meta = prepare_object_launch(meta, table, name, operation)
    logger.debug("id of table %s", id(table))
    logger.debug("operation from meta %s", meta["fmu"]["aggregation"])
    logger.debug("cols from meta %s", meta["data"]["spec"]["columns"])
    path = f"/objects('{parent_id}')"
    rsp_code = "0"
    success_response = (200, 201)
    while rsp_code not in success_response:
        try:
            response = sumo.post(path=path, json=meta)
            rsp_code = response.status_code
            logger.debug("response meta: %s", rsp_code)
        except Exception:
            exp_type, _, _ = sys.exc_info()
            logger.debug("Exception %s while uploading metadata", str(exp_type))

    blob_url = response.json().get("blob_url")
    rsp_code = "0"
    while rsp_code not in success_response:
        try:
            response = sumo.blob_client.upload_blob(blob=byte_string, url=blob_url)
            rsp_code = response.status_code
            logger.debug("Response blob %s", rsp_code)
        except Exception:
            exp_type, _, _ = sys.exc_info()
            logger.debug("Exception %s while uploading metadata", str(exp_type))


def upload_stats(
    sumo: SumoClient, parent_id: str, stat_input: list, meta: dict, loop, executor
):
    """Generate set of coroutine tasks for uploads

    Args:
        sumo (SumoClient): initialized sumo client
        parent_id (str): sumo id of parent object
        stat_input (list): list of tuples with name of table, and table
        meta (dict): metadata stub
        loop (ayncio.event_loog): Event loop to run coroutines
        executor (ThreadPoolExecutor): Executor for event loop

    Returns:
        list: list of coroutines
    """
    logger = init_logging(__name__ + ".upload_stats")
    tasks = []
    logger.info("%s tables to upload", len(stat_input))

    for item in stat_input:

        operation, table = item
        name = table.column_names.pop()
        tasks.append(
            call_parallel(
                loop,
                executor,
                upload_table,
                sumo,
                parent_id,
                table,
                name,
                meta,
                operation,
            )
        )
    return tasks


async def extract_and_upload(
    sumo: SumoClient,
    parent_id: str,
    table: pa.Table,
    table_index: list,
    meta_stub: dict,
    loop,
    executor,
):
    """Split pa.Table into seperate parts

    Args:
        sumo (SumoClient): initialized sumo client
        parent_id (str): object id of parent object
        table (pa.Table): The table to split
        table_index (list): the columns in the table defining the index
        meta_stub (dict): a metadata stub to be used for generating metadata for all split results
        loop (asyncio.event_loop): event loop to be used for upload
        executor (ThreadpoolExecutor): Executor for event loop
        keep_grand_aggregation (bool, optional): Upload the large aggregation as object.
                                                Defaults to False.
    """
    logger = init_logging(__name__ + ".extract_and_upload")
    logger.debug(
        "Opening the show with a table consisting of columns %s", table.column_names
    )
    count = 0
    neccessaries = table_index + ["REAL"]
    unneccessaries = ["YEARS", "SECONDS", "ENSEMBLE"]
    logger.info("This is the index to keep %s", neccessaries)
    # task scheduler
    tasks = []
    table_dict = {}
    for col_name in table.column_names:
        if col_name in (neccessaries + unneccessaries):
            continue
        logger.debug("Preparing %s", col_name)
        keep_cols = table_index + [col_name]
        logger.debug("Columns to pass through %s", keep_cols)
        export_frame = table.select(keep_cols)
        table_dict[col_name] = export_frame
        tasks.append(
            call_parallel(
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
        )
        count += 1
    logger.debug("Submitting: %s", table_dict)

    tasks.extend(
        upload_stats(
            sumo,
            parent_id,
            make_stat_aggregations(table_dict, table_index),
            meta_stub,
            loop,
            executor,
        )
    )

    logger.info("Tasks to run %s ", len(tasks))
    await asyncio.gather(*tasks)
    logger.info("%s objects produced", count * 6 + 2)


def convert_metadata(
    single_metadata: dict,
    real_ids: list,
    table_index,
    operation: str = "collection",
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
    if table_index is not None:

        agg_metadata["data"]["table_index"] = table_index
    else:
        try:
            table_index = agg_metadata["data"]["table_index"]
        except KeyError:
            logger.warning("No table index set, will produce no results")
            agg_metadata["data"]["table_index"] = None

    # Adding specific aggregation ones
    agg_metadata["fmu"]["aggregation"] = agg_metadata["fmu"].get("aggregation", {})
    agg_metadata["fmu"]["aggregation"]["operation"] = operation
    agg_metadata["fmu"]["aggregation"]["realization_ids"] = list(real_ids)
    agg_metadata["fmu"]["context"]["stage"] = "iteration"
    # Since no file on disk, trying without paths
    agg_metadata["file"]["absolute_path"] = ""
    agg_metadata["data"]["spec"]["columns"] = []

    return agg_metadata
