"""Dispatches jobs to to radix"""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from sumo.wrapper import SumoClient
import sumo.table_aggregation.utilities as ut
from httpx import HTTPStatusError


def query_for_it_name_and_tags(sumo: SumoClient, case_uuid: str, pit):
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
                ],
                "must_not": [
                    {
                        "term": {
                            "fmu.aggregation.operation.keyword": {"value": "collection"}
                        }
                    }
                ],
            }
        },
        "aggs": {
            "iter": {
                "terms": {"field": "fmu.iteration.name.keyword", "size": 100},
                "aggs": {
                    "name": {
                        "terms": {"field": "data.name.keyword", "size": 100},
                        "aggs": {
                            "tagname": {
                                "terms": {"field": "data.tagname.keyword", "size": 100}
                            }
                        },
                    }
                },
            }
        },
        "size": 0,
    }
    logger.debug("\nSubmitting query for tags: %s\n", query)
    results = sumo.post("/search", json=query)
    logger.debug("\nQuery results all\n %s", results)
    logger.debug(
        "\nQuery results\n %s", results.json()["aggregations"]["iter"]["buckets"]
    )
    return results["aggregations"]["iter"]["buckets"]


def collect_it_name_and_tag(sumo, uuid, pit):
    """Make dict with key as table name, and value list of corresponding tags

    Args:
        sumo (SumoClient): Initialized sumo client
        case_uuid (str): uuid for case
        iteration (str): iteration name
        pit (str): point in time id

    Returns:
        dict: the results
    """
    iterations = query_for_it_name_and_tags(sumo, uuid, pit)
    its_names_and_tags = {}
    for iteration in iterations:
        it_key = iteration["key"]
        its_names_and_tags[it_key] = {}
        name_buckets = iteration["name"]["buckets"]
        for name_bucket in name_buckets:
            name_key = name_bucket["key"]
            its_names_and_tags[it_key][name_key] = []
            tag_buckets = name_bucket["tagname"]["buckets"]
            for tag_bucket in tag_buckets:
                its_names_and_tags[it_key][name_key].append(tag_bucket["key"])

    return its_names_and_tags


def query_for_columns(sumo, uuid, name, tagname, pit, size=200):
    """Find column names for given combination of name and tagname for a table

    Args:
        sumo (SumoClient): Client for given sumo environment
        uuid (str): uuid for a given case
        name (str): name for a specific table
        tagname (str): tagname for a given table
        size (int, optional): size of query. Defaults to 200.

    Returns:
        list: all available columns in the table combination
    """
    logger = ut.init_logging(__name__ + ".query_for_columns")

    query = (
        f"_sumo.parent_object:{uuid} AND data.name:{name} AND data.tagname:{tagname} "
        + "AND NOT fmu.aggregation.operation:*"
    )
    select = "data.spec.columns"
    logger.debug("Submitting query: %s", query)
    p_dict = {"$query": query, "$size": size, "$pit": pit, "$select": select}
    results = sumo.get("/search", p_dict).json()
    logger.debug("----\n Returned %s hits\n-----", len(results["hits"]["hits"]))

    all_cols = set()
    for hit in results["hits"]["hits"]:
        all_cols.update(hit["_source"]["data"]["spec"]["columns"])

    return list(all_cols)


def list_of_list_segments(sumo, uuid, name, tagname, table_index, pit, seg_length=1000):
    """Return a list of list segments from given one table

    Args:
        sumo (SumoClient): Client to given sumo environment
        uuid (str): uuid for case
        name (str): name of table
        tagname (str): tagname of table
        table_index (list): table index for table
        pit (str): point in time id
        seg_length (int, optional): max length of segments. Defaults to 1000.

    Returns:
        list: list with lists that are segments of the columns available in table
    """
    long_list = query_for_columns(sumo, uuid, name, tagname, pit)
    segmented_list = []
    for segment in ut.split_list(long_list, seg_length):
        segment_set = set(segment)
        segment_set.update((table_index))
        segmented_list.append(list(segment_set))

    return segmented_list


def generate_dispatch_info(uuid, env, token=None, pit=None, seg_length=1000):
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
    it_name_and_tag = collect_it_name_and_tag(sumo, uuid, pit)
    logger.debug("---------")
    logger.debug(it_name_and_tag)
    print(it_name_and_tag)
    logger.debug("---------")
    for iter_name, it_tables in it_name_and_tag.items():
        logger.debug(iter_name)
        logger.debug(it_tables)
        dispatch_combination = {}
        dispatch_combination["uuid"] = uuid
        dispatch_combination["iter_name"] = iter_name
        for table_name, tag_names in it_tables.items():
            logger.debug(table_name)
            logger.debug(tag_names)
            dispatch_combination["table_name"] = table_name
            for tag_name in tag_names:
                try:
                    (
                        dispatch_combination["object_ids"],
                        dispatch_combination["base_meta"],
                        dispatch_combination["table_index"],
                    ) = ut.query_for_table(
                        sumo, uuid, table_name, tag_name, iter_name, pit
                    )
                except HTTPStatusError:
                    logger.warning(
                        "Cannot get results for comination (%s, %s)",
                        table_name,
                        tag_name,
                    )
                    continue

                dispatch_combination["base_meta"]["data"]["spec"]["columns"] = []
                dispatch_combination["tag_name"] = tag_name
                for col_segment in list_of_list_segments(
                    sumo,
                    uuid,
                    table_name,
                    tag_name,
                    dispatch_combination["table_index"],
                    pit,
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
