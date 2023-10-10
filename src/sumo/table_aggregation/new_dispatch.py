"""Dispatches jobs to to radix"""
from sumo.wrapper import SumoClient
import sumo.table_aggregation.utilities as ut
from copy import deepcopy


def query_for_it_name_and_tags(sumo: SumoClient, case_uuid: str, pit):
    """Query sumo for iteration name, and corresponding combinations of names and tagnames

    Args:
        sumo (SumoClient): Client to a given sumo environment
        case_uuid (str): uuid of a specific case
        pit (sumo.pit): point in time

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
                    },
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
                                "terms": {"field": "data.tagname.keyword", "size": 100},
                            }
                        },
                    }
                },
            }
        },
        "size": 0,
        # "pit": pit,
    }
    logger.debug("\nSubmitting query for tags: %s\n", query)
    results = sumo.post("/search", json=query).json()
    logger.debug("\nQuery results\n %s", results["aggregations"]["iter"]["buckets"])
    return results["aggregations"]["iter"]["buckets"]


def collect_it_name_and_tag(sumo, uuid, pit):
    """Make dict with key as table name, and value list of corresponding tags

    Args:
        sumo (SumoClient): Initialized sumo client
        case_uuid (str): uuid for case
        iteration (str): iteration name
        pit (sumo.pit): point in time

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
    query = (
        f"_sumo.parent_object:{uuid} AND data.name:{name} AND data.tagname:{tagname} "
        + "AND NOT fmu.aggregation.operation:*"
    )
    select = "data.spec.columns"
    results = sumo.get("/search", query=query, select=select, size=size, pit=pit)
    all_cols = set()
    for hit in results["hits"]["hits"]:
        all_cols.update(hit["_source"]["data"]["spec"]["columns"])

    return list(all_cols)


def generate_dispatch_info(uuid, env, token=None, pit=None, seg_length=1000):
    """Generate dispatch info for all batch jobs to run

    Args:
        uuid (str): case uuid
        env (str): name of sumo env to read from
        seg_length (str): length of columns to pass per batch job
        pit (sumo.pit, optional): point in time, Defaults to None

    Returns:
        list: list of all table combinations
    """
    sumo = SumoClient(env, token)
    if pit is None:
        pit = sumo.post("/pit", params={"keep-alive": "1m"}).json()["id"]

    dispatch_info = []
    for iter_name, it_tables in collect_it_name_and_tag(sumo, uuid, pit).items():
        dispatch_combination = {}
        dispatch_combination["uuid"] = uuid
        dispatch_combination["iter_name"] = iter_name
        for table_name, tag_names in it_tables.items():
            dispatch_combination["table_name"] = table_name
            for tag_name in tag_names:
                dispatch_combination["tag_name"] = tag_name
                for col_segment in ut.split_list(
                    query_for_columns(sumo, uuid, table_name, tag_name, pit), seg_length
                ):
                    dispatch_combination["columns"] = col_segment
                    dispatch_info.append(deepcopy(dispatch_combination))
    return dispatch_info
