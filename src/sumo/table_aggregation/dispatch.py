"""Dispatches jobs to to radix"""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from sumo.wrapper import SumoClient
import sumo.table_aggregation.utilities as ut
from httpx import HTTPStatusError


def query_for_names_and_tags(
    sumo: SumoClient, case_uuid: str, iter_name: str = "0", pit=None
):
    """Query sumo for iteration name, and corresponding combinations of names and tagnames

    Args:
        sumo (SumoClient): Client to a given sumo environment
        case_uuid (str): uuid of a specific case
        pit (str): point in time id

    Returns:
        dict: query results
    """
    logger = ut.init_logging(__name__ + ".query_for_it_name_and_tags")
    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"_sumo.parent_object.keyword": {"value": case_uuid}}},
                    {"term": {"class.keyword": {"value": "table"}}},
                    {"term": {"fmu.iteration.name.keyword": {"value": iter_name}}},
                    {"term": {"fmu.context.stage.keyword": {"value": "realization"}}},
                ],
            }
        },
        "aggs": {
            "name": {
                "terms": {"field": "data.name.keyword", "size": 100},
                "aggs": {
                    "tagname": {
                        "terms": {"field": "data.tagname.keyword", "size": 100},
                    },
                },
            }
        },
        "size": 0,
    }
    # if pit is not None:
    #     query["pit"] = pit
    logger.debug("\nSubmitting query for tags: %s\n", query)
    results = sumo.post("/search", json=query).json()
    logger.debug("\nQuery results\n %s", results["aggregations"]["name"]["buckets"])
    return results["aggregations"]["name"]["buckets"]


def collect_names_and_tags(sumo, uuid, iteration, pit=None):
    """Make dict with key as table name, and value list of corresponding tags

    Args:
        sumo (SumoClient): Initialized sumo client
        case_uuid (str): uuid for case
        iteration (str): iteration name
        pit (str): point in time id

    Returns:
        dict: the results
    """
    names = query_for_names_and_tags(sumo, uuid, iteration, pit)
    names_and_tags = {}
    for name in names:
        name_key = name["key"]
        tag_buckets = name["tagname"]["buckets"]
        names_and_tags[name_key] = []
        for tag_bucket in tag_buckets:
            names_and_tags[name_key].append(tag_bucket["key"])

    return names_and_tags


def list_of_list_segments(metadata, seg_length=250):
    """Return a list of list segments from given one table

    Args:
        metadata: metadata for a single realization
        seg_length (int, optional): max length of segments. Defaults to 1000.

    Returns:
        list: list with lists that are segments of the columns available in table
    """
    long_list = metadata["data"]["spec"]["columns"]
    segmented_list = []
    for segment in ut.split_list(long_list, seg_length):
        segment_set = set(segment)
        segment_set.update(metadata["data"]["table_index"])
        segmented_list.append(list(segment_set))

    return segmented_list


def generate_dispatch_info(
    uuid, env, iteration_name, token=None, pit=None, seg_length=250
):
    """Generate dispatch info for all batch jobs to run

    Args:
        uuid (str): case uuid
        env (str): name of sumo env to read from
        seg_length (str): length of columns to pass per batch job
        pit (str, optional): point in time id, Defaults to None

    Returns:
        list: list of all table combinations
    """
    logger = ut.init_logging(__name__ + ".generate_dispatch_info")
    sumo = SumoClient(env, token)
    if pit is None:
        pit = sumo.post("/pit", params={"keep-alive": "1m"}).json()["id"]

    dispatch_info = []
    name_and_tag = collect_names_and_tags(sumo, uuid, iteration_name, pit)
    logger.debug("---------")
    logger.debug(name_and_tag)
    logger.debug("---------")
    dispatch_combination = {}
    dispatch_combination["uuid"] = uuid
    # dispatch_combination["iter_name"] = iteration_name
    # for iter_name, it_tables in it_name_and_tag.items():
    #     logger.debug(iter_name)
    #     logger.debug(it_tables)
    for table_name, tag_names in name_and_tag.items():
        logger.debug(table_name)
        logger.debug(tag_names)
        dispatch_combination["table_name"] = table_name
        for tag_name in tag_names:
            try:
                (
                    dispatch_combination["object_ids"],
                    base_meta,
                    dispatch_combination["table_index"],
                ) = ut.query_for_table(
                    sumo, uuid, table_name, tag_name, iteration_name, pit
                )
            except HTTPStatusError:
                logger.warning(
                    "Cannot get results for combination (%s, %s)",
                    table_name,
                    tag_name,
                )
                continue

            dispatch_combination["tag_name"] = tag_name
            for col_segment in list_of_list_segments(
                dispatch_combination["base_meta"],
                seg_length,
            ):
                dispatch_combination["columns"] = col_segment
                dispatch_info.append(deepcopy(dispatch_combination))
    return dispatch_info


def aggregate_and_upload(dispatch_info, sumo):
    """aggregate based on dispatch info

    Args:
        dispatch_info (dict): dictionary with all run info for one job
        sumo (SumoClient): client for given sumo environment
    """
    uuid = dispatch_info["uuid"]
    table_index = dispatch_info["table_index"]
    object_ids = dispatch_info["object_ids"]
    columns = dispatch_info["columns"]
    base_meta = dispatch_info["base_meta"]
    loop = asyncio.get_event_loop()
    aggregated = None
    if (table_index is not None) and (len(table_index) > 0):
        aggregated = loop.run_until_complete(
            ut.aggregate_arrow(
                object_ids,
                sumo,
                columns,
                loop,
            )
        )
        executor = ThreadPoolExecutor()
        loop.run_until_complete(
            ut.extract_and_upload(
                sumo,
                uuid,
                aggregated,
                table_index,
                base_meta,
                loop,
                executor,
            )
        )
