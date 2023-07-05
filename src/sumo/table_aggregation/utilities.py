"""Utils for table aggregation"""
import os
import base64
import json
import sys
import time
from datetime import datetime
import logging
import warnings
import hashlib
import uuid
from typing import Dict, Union
import asyncio
from multiprocessing import get_context
from copy import deepcopy
from io import BytesIO
import psutil
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
from pyarrow import feather
import pyarrow.parquet as pq
from sumo.wrapper import SumoClient
from sumo.wrapper._request_error import PermanentError


# inner psutil function
def process_memory():
    """Fetch memory usage"""
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    return mem_info.rss


# decorator function
def memcount():
    """Decorate function to monitor memory usage"""
    logger = init_logging(__name__ + ".memcount")

    def decorator(func):
        def wrapper(*args, **kwargs):
            mem_before = process_memory()
            result = func(*args, **kwargs)
            mem_after = process_memory()
            logger.info(
                "Memory used by %s: in %i, out %i, difference  %i ",
                func.__name__,
                mem_before,
                mem_after,
                mem_after - mem_before,
            )

            return result

        return wrapper

    return decorator


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


def split_list(list_to_split: list, size: int) -> list:
    """Split list into segments

    Args:
        list_to_split (list): the list to split
        size (int): the size of each sublist

    Returns:
        list: the list of lists
    """
    list_list = []
    while len(list_to_split) > size:
        piece = list_to_split[:size]
        list_list.append(piece)
        list_to_split = list_to_split[size:]
    list_list.append(list_to_split)
    return list_list


def init_logging(name: str) -> logging.Logger:
    """Init logging null handler
    args:
    name (str): name of logger
    returns (logging.Logger): an initialized logger
    """
    logger = logging.getLogger(name)
    logger.addHandler(logging.NullHandler())
    return logger


def get_expiry_time(sumo: SumoClient) -> int:
    """Get expiry time from sumo client

    Args:
        sumo (SumoClient): The activated client

    Returns:
        int: time since epoch in seconds
    """
    logger = init_logging(__name__ + ".get_expiry_time")
    token_parts = sumo._retrieve_token().split(".")
    body = json.loads(base64.b64decode(token_parts[1]).decode(encoding="utf-8"))

    expiry_time = body["exp"]
    strftime = datetime.fromtimestamp(expiry_time).strftime("%Y-%m-%d %H:%M:%S")
    logger.debug("Token expires at %s", strftime)
    return expiry_time


def find_env(url):
    """Return sumo environment

    Args:
        url (str): the base url of sumo client

    Returns:
        str: the name of environments
    """
    logger = init_logging(__name__ + ".find_url")
    logger.debug("Finding env from url: %s", url)
    url_parts = url.split(".")
    return url_parts[0].split("-")[-1]


def check_or_refresh_token(sumo):
    """Checks whether token is about to expire

    Args:
        sumo (SumoClient): the client to check against

    Raises:
        TimeoutError: if the token has expired

    Returns:
        sumo: _description_
    """
    logger = init_logging(__name__ + ".check_or_refresh_token")
    expiry_time = get_expiry_time(sumo)
    current_time = time.time()
    lim_in_min = 10
    limit = (expiry_time - current_time) / (60 * lim_in_min)
    logger.debug("%s to go ", f"{limit: 3.1f}")
    if limit < 0:
        logger.critical("Oh no too late! No token")
        raise TimeoutError("To late!!!")
    if limit < 10:
        logger.info("Refreshing token")
        sumo.auth.get_token()
        # sumo = SumoClient(find_env(sumo.base_url))
    else:
        logger.debug("No worries here")
    return sumo


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
    """Return fmu.case.uuid, either via name, or just pass on
    args:
    identifier (str): either case name of case uuid (prefered)
    version (int): what version of uuid to compare to
    """
    # Concepts stolen from stackoverflow.com
    # questions/19989481/how-to-determine-if-a-string-is-a-valid-v4-uuid
    logger = init_logging(__name__ + ".return_uuid")
    logger.debug("Checking %s", identifier)
    try:
        logger.debug("Checking for uuid")
        uuid.UUID(identifier, version=version)
    except ValueError:
        logger.warning("%s should be the name of a case", identifier)
        warnings.warn(
            "Using case name: this is not the prefered option,"
            "might in the case of duplicate case names give errors"
        )
        logger.debug("Passing %s to return a uuid", identifier)
        identifier = query_for_sumo_id(sumo, identifier)
        logger.debug("After query we are left with %s", identifier)
    return identifier


def query_for_sumo_id(sumo: SumoClient, case_name: str) -> str:
    """Find uuid for given case name

    Args:
        sumo (SumoClient): initialized sumo client
        case_name (str): name of case

    Returns:
        str: case uuid
    """
    logger = init_logging(__name__ + ".query_for_sumo_id")
    select = "fmu.case.uuid"
    query = f"fmu.case.name:{case_name}"
    results = sumo.get(
        path="/searchroot",
        query=query,
        size=1,
        select=select,
    )
    logger.debug("%s hits.", len(results["hits"]["hits"]))
    unique_id = results["hits"]["hits"][0]["_source"]["fmu"]["case"]["uuid"]
    return unique_id


def get_buckets(agg_results, selector):
    """Fetch unique combinations in aggregated results

    Args:
        agg_results (dict): dict of results["aggregations"]
        selector (str): name of buckets

    Returns:
        list: list of results
    """

    agg_list = [bucket["key"] for bucket in agg_results[selector]["buckets"]]
    return agg_list


def query_sumo_iterations(sumo: SumoClient, case_uuid: str) -> list:
    """Qeury for iteration names

    Args:
        sumo (SumoClient): initialized sumo client
        case_uuid (str): name of case

    Returns:
        list: list with iteration numbers
    """
    logger = init_logging(__name__ + ".query_sumo_iterations")
    query = f"\nfmu.case.uuid:{case_uuid}\n"
    logger.debug(query)
    selector = "fmu.iteration.name"
    bucket_name = selector + ".keyword"
    results = sumo.get(
        path="/search",
        query=query,
        size=0,
        select=selector,
        buckets=bucket_name,
    )
    iterations = get_buckets(results["aggregations"], bucket_name)
    return iterations


def query_for_name_and_tags(sumo: SumoClient, case_uuid: str, iteration: str):
    """Make dict with key as table name, and value list of corresponding tags

    Args:
        sumo (SumoClient): Initialized sumo client
        case_uuid (str): uuid for case
        iteration (str): iteration name

    Returns:
        dict: the results
    """
    logger = init_logging(__name__ + ".query_for_name_and_tags")
    logger.info("Finding tables for iteration: %s", iteration)
    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"_sumo.parent_object.keyword": {"value": case_uuid}}},
                    {"term": {"class.keyword": {"value": "table"}}},
                    {"term": {"fmu.iteration.name.keyword": {"value": iteration}}},
                ],
                "must_not": [{"term": {"data.tagname.keyword": {"value": ""}}}],
            }
        },
        "aggs": {
            "table": {
                "terms": {"field": "data.name.keyword", "size": 100},
                "aggs": {
                    "tagname": {"terms": {"field": "data.tagname.keyword", "size": 100}}
                },
            }
        },
        "size": 0,
    }
    logger.debug("\nSubmitting query for tags: %s\n", query)
    results = sumo.post("/search", json=query).json()
    logger.debug("\nQuery results\n %s", results)

    name_with_tags = {}
    for hit in results["aggregations"]["table"]["buckets"]:
        logger.debug(hit["key"])
        name = hit["key"]
        name_with_tags[name] = name_with_tags.get(name, [])
        for taghit in hit["tagname"]["buckets"]:
            name_with_tags[name].append(taghit["key"])
    logger.info("These are the names and tags:\n%s", name_with_tags)
    return name_with_tags


def query_sumo(
    sumo: SumoClient,
    case_uuid: str,
    name: str,
    tag: str,
    iteration: str,
    pit: str,
    search_after=None,
) -> dict:
    """Query for given table type

    Args:
        sumo (SumoClient): initialized sumo client
        case_uuid (str): case uuid
        name (str): name of table
        iteration (str): iteration number
        tag (str): tagname of table. Defaults to "".
        pit (str): id for point in time

    Returns:
        dict: query results
    """
    logger = init_logging(__name__ + ".query_sumo")
    logger.debug(
        "At query: id: %s, name: %s, tag: %s, it: %s",
        case_uuid,
        name,
        tag,
        iteration,
    )
    buck_term = "file.checksum_md5.keyword"
    query = (
        f"class:table AND _sumo.parent_object:{case_uuid}"
        + f" AND data.name:{name} AND data.tagname:{tag}"
        + f" AND fmu.iteration.name:{iteration}"
        + " AND NOT fmu.aggregation.operation:*"
    )
    if search_after is None:
        query_results = sumo.get(
            path="/search",
            query=query,
            sort="_doc:asc",
            pit=pit,
            size=100,
            buckets=buck_term,
        )
    else:
        query_results = sumo.get(
            path="/search",
            query=query,
            size=100,
            sort="_doc:asc",
            pit=pit,
            search_after=json.dumps(search_after),
            buckets=buck_term,
        )
    logger.info("Query: %s", query)
    buckets = get_buckets(query_results["aggregations"], buck_term)

    return query_results, buckets


def query_for_table(
    sumo: SumoClient,
    case_uuid: str,
    name: str,
    tag: str,
    iteration: str,
    **kwargs: dict,
) -> tuple:
    """Fetch object id numbers and metadata

    Args:
        sumo (SumoClient): intialized sumo client
        case_uuid (str): case uuid
        name (str): name of table
        tag (str, optional): tagname of table. Defaults to "".
        iteration (str): iteration number


    Raises:
        RuntimeError: if no tables found

    Returns:
        tuple: contains parent id, object ids, meta data stub, all real numbers
               and dictionary containing all global variables for all realizations
    """
    logger = init_logging(__name__ + ".query_for_table")
    logger.debug(
        "Passing to query: id: %s, name: %s, tag: %s, it: %s",
        case_uuid,
        name,
        tag,
        iteration,
    )
    unique_buck = set()
    pit = sumo.post("/pit", params={"keep-alive": "1m"}).json()["id"]
    query_results, buck = query_sumo(sumo, case_uuid, name, tag, iteration, pit)
    total_hits = query_results["hits"]["total"]["value"]
    if total_hits == 0:
        raise RuntimeError("Query returned with no hits, if you want results: modify!")
    hits = query_results["hits"]["hits"]
    unique_buck.update(buck)

    while len(hits) < total_hits:
        query_results, more_buck = query_sumo(
            sumo, case_uuid, name, tag, iteration, pit, hits[-1]["sort"]
        )
        hits.extend(query_results["hits"]["hits"])
        unique_buck.update(more_buck)
        logger.debug("hits actually contained in request: %s", len(hits))

    if len(unique_buck) == 1:
        logger.warning(
            "Name: %s and tag %s, all objects are equal, will only pass one", name, tag
        )
        hits = hits[:1]
    results = get_blob_ids_w_metadata(hits, **kwargs)
    return results


def uuid_from_string(string: str) -> str:
    """Generate uuid from string

    Args:
        string (str): string to generate from

    Returns:
        str: uuid which is hash of md5
    """
    return str(uuid.UUID(hashlib.md5(string.encode("utf-8")).hexdigest()))


def get_object(object_id: str, cols_to_read: list, sumo: SumoClient) -> pa.Table:
    """fetche sumo object as pa.Table

    Args:
        object_id (str): sumo object id
        sumo (SumoClient): client to a given environment

    Returns:
        pa.Table: the object as pyarrow
    """
    query = f"/objects('{object_id}')/blob"
    try:
        table = arrow_to_table(sumo.get(query), cols_to_read)
    except (PermanentError, ConnectionError):
        time.sleep(0.5)
        table = get_object(object_id, cols_to_read, sumo)

    return table


def arrow_to_table(blob_object, columns) -> pa.Table:
    """Reads sumo blob into pandas dataframe
    args:
    blob_object (dict): the object to read
    pa.Table: the read results
    """
    logger = init_logging(__name__ + ".arrow_to_table")

    try:
        table = pq.read_table(pa.BufferReader(blob_object), columns=columns)
    except pa.lib.ArrowInvalid:
        try:
            table = feather.read_table(pa.BufferReader(blob_object), columns=columns)
        except pa.lib.ArrowInvalid:
            logger.warning(
                "Reading csv file with just some columns, not as efficent as arrow"
            )
            frame = pd.read_csv(BytesIO(blob_object))
            table = pa.Table.from_pandas(frame[columns])
    logger.debug("Returning table with %s columns", len(table.column_names))
    return table


class MetadataSet:

    """Class for arrangement of input to aggregation"""

    def __init__(self, table_index=None):
        """Sets _parameter_dict to empty dict"""
        self._parameter_dict = {}
        self._real_ids = set()
        self._uuids = set()
        self._table_index = table_index
        self._base_meta = {}
        self._columns = ()
        self._logger = init_logging(__name__ + ".MetadataSet")

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

    @property
    def base_meta(self):
        """Return attribute _base_meta

        Returns:
            dict: metadata to be used as basis for all aggregated objects
        """
        return self._base_meta

    @property
    def agg_columns(self):
        """Return columns representing all realizations

        Returns:
            list: list of all columns in specific table
        """
        return self._columns

    @agg_columns.setter
    def agg_columns(self, columns):
        self._columns = columns

    def resolve_col_conflicts(self, columns, realnr):
        """Check if columns for specific real matches the other reals

        Args:
            columns (list): list of cols for specific objecty
            realnr (int): realization nr
        """
        if (len(self._columns) > 0) & (len(self._columns) != len(columns)):
            self._logger.info("Need to resolve columns")
            if len(self.agg_columns) < len(columns):
                first = columns
                second = self.agg_columns
            else:
                first = self.agg_columns
                second = columns
            diff = [col_name for col_name in first if col_name not in second]
            if len(diff) > 0:
                mess = (
                    f"This/these columns are not in all reals {diff}"
                    + f", something is different with real {realnr}"
                )
                # warnings.warn(mess)
                self._logger.info(mess)

            self.agg_columns = first
        else:
            self._columns = columns
            self._logger.info("Columns are good")

    def aggid(self) -> str:
        """Return the hash of the sum of all the sorted(uuids)"""
        return str("".join(sorted(self.uuids)))

    def add_realisation(self, real_nr: int):
        """Adds realnr for relevant real
        args:
        real_nr (int):real nr
        """
        self._real_ids.add(real_nr)

    def gen_agg_meta(self, metadata: dict) -> dict:
        """Converts one metadata file into aggregated metadata
        args:
        metadata (dict): one valid metadatafile
        returns agg_metadata (dict): one valid metadata file to be used for
                                     aggregations to come
        """
        logger = init_logging(__name__ + ".base_meta")
        self._base_meta = convert_metadata(
            metadata, self.real_ids, self.parameter_dict, self.table_index
        )
        self._base_meta["data"]["spec"]["columns"] = self.agg_columns
        self._table_index = self._base_meta["data"]["table_index"]

        logger.debug("--\n Table index is: %s\n--------", self._table_index)


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
    col_lengths = set()
    meta = MetadataSet(kwargs.get("table_index", None))
    blob_ids = {}

    for result in results:
        real_meta = result["_source"]
        found_cols = real_meta["data"]["spec"]["columns"]
        col_lengths.add(len(found_cols))
        try:
            real = real_meta["fmu"].pop("realization")
            realnr = real["id"]
        except KeyError:
            logger.warning("No realization in result, already aggregation?")
            continue
        meta.resolve_col_conflicts(found_cols, realnr)
        meta.add_realisation(realnr)

        blob_ids[realnr] = result["_id"]
    logger.debug(col_lengths)
    if len(col_lengths) != 1:
        logger.warning(
            "Several sets of columns (%s) see difference in lengths: \n%s",
            len(col_lengths),
            col_lengths,
        )
    meta.gen_agg_meta(real_meta)

    split_tup = (
        parent_id,
        blob_ids,
        meta.base_meta,
        meta.table_index,
    )
    return split_tup


def get_blob_ids_w_metadata(hits: list, **kwargs: dict) -> tuple:
    """Get all object ids and metadata for iteration

    Args:
        query_results (dict): results from sumo query

    Returns:
        tuple: see under split results_and_meta
    """
    logger = init_logging(__name__ + ".get_blob_ids_w_meta")

    logger.info("hits actually contained in request: %s", len(hits))

    return split_results_and_meta(hits, **kwargs)


@memcount()
def reconstruct_table(
    object_id: str, real_nr: str, sumo: SumoClient, required: list
) -> pa.Table:
    """Reconstruct pa.Table from sumo object id

    Args:
        object_id (str): the object to fetch
        real_nr (str): the real nr of the object
        sumo (SumoClient): initialized sumo client
        required (list): list of columns that need to be in table


    Returns:
        pa.Table: The table
    """
    logger = init_logging(__name__ + ".reconstruct_table")
    logger.debug("Real %s", real_nr)
    real_table = get_object(object_id, required, sumo)
    rows = real_table.shape[0]

    real_table = real_table.add_column(0, "REAL", pa.array([np.int16(real_nr)] * rows))
    missing = [
        col_name for col_name in required if col_name not in real_table.column_names
    ]
    if len(missing):
        logger.info("Real: %s, missing these columns %s", real_nr, missing)
    for miss in missing:
        real_table = real_table.add_column(0, miss, pa.array([None] * rows))
    logger.debug("Table created %s", type(real_table))
    # Sort to ensure that table has cols in same order even
    # when missing cols occur

    return real_table.select(sorted(real_table.column_names))


async def aggregate_arrow(
    object_ids: Dict[str, str], sumo: SumoClient, required, loop
) -> pa.Table:
    """Aggregate the individual objects into one large pyarrow table
    args:
    object_ids (dict): key is real nr, value is object id
    sumo (SumoClient): initialized sumo client
    required (list): list of columns that need to be in table
    loop (asyncio.event_loop)
    returns: pa.Table: the aggregated results
    """
    logger = init_logging(__name__ + ".aggregate_arrow")
    aggregated = []
    for real_nr, object_id in object_ids.items():
        aggregated.append(
            call_parallel(
                loop, None, reconstruct_table, object_id, real_nr, sumo, required
            )
        )
    logger.info("Ready for action!")
    aggregated = pa.concat_tables(await asyncio.gather(*aggregated), promote=True)
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
    logger.debug("Nr of columns prior to groupby: %s", len(frame.columns))
    stat = frame.groupby(index)[col_name].agg(aggfunc).to_frame().reset_index()
    table = pa.Table.from_pandas(stat)
    output = (aggname, table)
    logger.debug("%s %s", output[0], len(output[1].column_names))
    return output


@timethis("multiprocessing")
@memcount()
def make_stat_aggregations(
    col_name: str,
    table: pa.Table,
    table_index: Union[list, str],
    # aggfuncs: list = ("mean", "min", "max", p10, p90),
):
    """Return statistical aggregations from pyarrow table

    Args:
        col_name (str): name of column to perform stats on
        table (pa.Table): The table to do the stats from
        table_index (Union[list, str]): name of table index columns

    Returns:
        tuple: results from statistical calculations
    """
    logger = init_logging(__name__ + ".make_stat_aggregations")
    logger.debug("Running with %s cpus", os.cpu_count())
    aggdict = {"mean": "mean", "min": "min", "max": "max", "p10": p10, "p90": p90}
    stat_input = []

    logger.debug("----converting to pandas---")
    # logger.debug(table.schema)
    frame = deepcopy(
        table.to_pandas(
            ignore_metadata=True,
        )
    )
    if frame[col_name].dtype != object:
        logger.debug("%s is not numeric, will not permform stats", col_name)

        logger.debug(
            "Nr of columns after conversion to pandas df %s (shape %s)",
            len(frame.columns),
            frame.shape,
        )
        stat_input.extend(
            [
                (frame, table_index, col_name, aggfunc, aggname)
                for aggname, aggfunc in aggdict.items()
            ]
        )

        logger.debug("Submitting %s tasks to multprocessing", len(stat_input))

        stats = pa.Table.from_arrays(pa.array([]))
        with get_context("spawn").Pool() as pool:
            stats = pool.starmap(do_stats, stat_input)
    else:
        stats = ()
    return stats


def prepare_object_launch(meta: dict, table, name, operation):
    """Complete metadata for object
    args:
    frame (pd.DataFrame): the data to write
    agg_meta (dict): Stub for aggregated meta to be written
    columns (list): the column names in the frame
    """
    logger = init_logging(__name__ + ".complete_meta")
    logger.debug("Preparing with data source %s", type(table))
    byte_string = table_to_bytes(table)
    tag = meta["data"]["tagname"]
    md5 = md5sum(byte_string)
    full_meta = deepcopy(meta)
    parent = full_meta["data"]["name"]
    logger.debug("Name prior to change: %s", parent)
    unique_name = (
        parent
        + f"--{name}--{tag}--{operation}--"
        + full_meta["fmu"]["iteration"]["name"]
    )
    full_meta["file"]["checksum_md5"] = md5
    full_meta["fmu"]["aggregation"]["id"] = uuid_from_string(md5)
    full_meta["fmu"]["aggregation"]["operation"] = operation
    full_meta["data"]["spec"]["columns"] = table.column_names
    if operation == "collection":
        full_meta["data"]["table_index"].append("REAL")
    # full_meta["data"]["name"] = name
    full_meta["display"]["name"] = name
    full_meta["file"]["relative_path"] = unique_name
    size = sys.getsizeof(full_meta) / (1024 * 1024)
    logger.info("Size of meta dict: %.2e\n", size)
    logger.debug("Metadata %s", full_meta)
    logger.debug("Object %s ready for launch", unique_name)
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
    # sumo = check_or_refresh_token(sumo)
    logger = init_logging(__name__ + ".upload_table")
    logger.debug("Uploading %s-%s", name, operation)
    logger.debug("Columns in table %s", table.column_names)
    logger.debug("Uploading to parent with id %s", parent_id)
    byte_string, meta = prepare_object_launch(meta, table, name, operation)
    logger.debug("id of table %s", id(table))
    logger.debug("operation from meta %s", meta["fmu"]["aggregation"])
    logger.debug("cols from meta %s", meta["data"]["spec"]["columns"])
    meta["data"]["spec"]["columns"] = []
    path = f"/objects('{parent_id}')"
    rsp_code = "0"
    success_response = (200, 201)
    meta_upload = True
    while rsp_code not in success_response:
        size_of_meta = sys.getsizeof(meta) / (1024 * 1024)
        try:
            response = sumo.post(path=path, json=meta)
            rsp_code = response.status_code
            logger.info("response meta: %s", rsp_code)
        except PermanentError:
            meta_upload = False
            logger.warning(
                "Permanent error while uploading metadata, Size of meta %.2e MB",
                size_of_meta,
            )
            logger.info(meta)
            break

        except Exception:
            exp_type, exp, _ = sys.exc_info()
            logger.warning(
                "Exception %s while uploading metadata (%s) (Size: %.2e MB)",
                exp,
                str(exp_type),
                size_of_meta,
            )
    if meta_upload:
        blob_url = response.json().get("blob_url")
        rsp_code = "0"
        while rsp_code not in success_response:
            try:
                response = sumo.blob_client.upload_blob(blob=byte_string, url=blob_url)
                rsp_code = response.status_code
                logger.info("Response blob %s", rsp_code)
            except Exception:
                exp_type, exp, _ = sys.exc_info()
                logger.warning(
                    "Exception %s while uploading metadata (%s)", exp, str(exp_type)
                )
        logger.info("uploaded %s", meta["file"]["relative_path"])
    else:
        logger.error("Cannot upload blob since no meta upload")


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
    logger.debug("%s tables to upload", len(stat_input))

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
    """
    logger = init_logging(__name__ + ".extract_and_upload")
    logger.debug(
        "Opening the show with a table consisting of columns %s", table.column_names
    )
    count = 0
    neccessaries = table_index + ["REAL"]
    unneccessaries = ["YEARS", "SECONDS", "ENSEMBLE"]
    logger.debug("This is the index to keep %s", neccessaries)
    # task scheduler
    tasks = generate_table_index_values(
        sumo, parent_id, table, table_index, meta_stub, loop, executor
    )
    for col_name in table.column_names:
        if col_name in (neccessaries + unneccessaries):
            continue
        table_dict = {}
        logger.debug("Preparing %s", col_name)
        keep_cols = neccessaries + [col_name]
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
        tasks.extend(
            upload_stats(
                sumo,
                parent_id,
                make_stat_aggregations(col_name, export_frame, table_index),
                meta_stub,
                loop,
                executor,
            )
        )

        count += 1
    logger.debug("Submitting: %s additional tasks", len(table_dict.keys()))

    logger.debug("Tasks to run %s ", len(tasks))
    await asyncio.gather(*tasks)
    logger.debug("%s objects produced", count * 6 + 2)


def convert_metadata(
    single_metadata: dict,
    real_ids: list,
    table_index,
    operation: str = "collection",
):
    """Make metadata for the aggregated data from single metadata
    args:
    single_metadata (dict): one single metadata dict
    real_ids (list): list of realization numbers, needed for metadata
    operation (str): what type of operation the aggregation performs
    returns agg_metadata (dict): metadata dict that can be further used for aggregation
    """
    logger = init_logging(__name__ + ".convert_metadata")
    logger.info(
        "Table index going in %s, but input index is %s",
        single_metadata["data"].get("table_index", None),
        table_index,
    )

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
    outside_index = False
    try:
        outside_index = len(table_index) > 0
    except TypeError:
        outside_index = table_index is not None
    if outside_index:
        agg_metadata["data"]["table_index"] = table_index
    else:
        try:
            table_index = agg_metadata["data"]["table_index"]
        except KeyError:
            logger.warning(
                "No table index set, will produce no results",
            )
            agg_metadata["data"]["table_index"] = None

    # Adding specific aggregation ones
    agg_metadata["fmu"]["aggregation"] = agg_metadata["fmu"].get("aggregation", {})
    agg_metadata["fmu"]["aggregation"]["operation"] = operation
    agg_metadata["fmu"]["aggregation"]["realization_ids"] = list(real_ids)
    agg_metadata["fmu"]["context"]["stage"] = "iteration"
    # Since no file on disk, trying without paths
    agg_metadata["file"]["absolute_path"] = ""
    agg_metadata["data"]["spec"]["columns"] = []
    logger.info("The table index will be %s", agg_metadata["data"]["table_index"])

    return agg_metadata


def generate_table_index_values(
    sumo,
    parent_id,
    table,
    table_index,
    meta_stub,
    loop,
    executor,
):
    """Que table_index values

    Args:
        sumo (SumoClient): initialized sumo Client
        parent_id (str): object id of parent object
        table (pa.Table): Table to derive indexes from
        table_index (list)): list of indexes
        meta_stub (dic): a metadata stub to be used for generating final meta
        loop (asyncio.event_loop): event loop to be used for upload
        executor (ThreadpoolExecutor): Executor for event loop

    Returns:
        list: tasks queued
    """
    index_tasks = []
    for index in table_index:
        ind_table = pa.Table.from_arrays([pc.unique(table[index])], names=[index])
        index_tasks.append(
            call_parallel(
                loop,
                executor,
                upload_table,
                sumo,
                parent_id,
                ind_table,
                index,
                meta_stub,
                "index",
            )
        )
    return index_tasks
