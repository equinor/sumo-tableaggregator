"""Dispatches jobs to to radix"""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from sumo.wrapper import SumoClient
import sumo.table_aggregation.utilities as ut


def init_sumo_env(env, token: str = None):
    """Initializes sumo environment

    Args:
        env (str): environment to run with
        token (str, optional): token to pass in. Defaults to None.

    Returns:
        SumoClient: The client for requested environment
    """
    logger = ut.init_logging(__file__ + ".init_sumo_env")
    sumo = SumoClient(env, token)
    logger.info("Init of client for %s", env)
    return sumo


def collect_case_table_name_and_tag(uuid, sumo):
    """Collect all table name and tag couplets for case, per real

    Args:
        uuid (str): universal unique id for case
        sumo (SumoClient): client to given environment

    Returns:
        dict: dict of dicts with first key as iteration, value as dict with
              name as key and value as list of tags
    """
    its_names_and_tags = {}
    iterations = ut.query_sumo_iterations(sumo, uuid)
    for iter_name in iterations:
        its_names_and_tags[iter_name] = ut.query_for_name_and_tags(
            sumo, uuid, iter_name
        )
    return its_names_and_tags


def generate_dispatch_info_per_combination(
    identifier, sumo, segment_length=1000, **kwargs
):
    """Generate list of job specicifations for one given table combination

    Args:
        identifier (tuple): tuple containing uuid, table name, table tagname, and iter name
        sumo (SumoClient): client to environment that contains case
        segment_length (int, optional): length to segment columns for table on. Defaults to 1000.

    Returns:
        list: list with combinations for job dispatch for one table combination
    """
    logger = ut.init_logging(__file__ + ".dispatch_info")
    uuid, name, tag, iteration = identifier
    table_dispatch_info = []
    table_specifics = {"uuid": uuid}
    (
        table_specifics["object_ids"],
        meta,
        table_index,
    ) = ut.query_for_table(sumo, uuid, name, tag, iteration, **kwargs)
    table_specifics["table_index"] = table_index
    segments = ut.split_list(meta["data"]["spec"]["columns"], segment_length)
    seg_specifics = table_specifics.copy()
    for segment in segments:
        seg_set = set(segment)
        try:
            seg_set.update(table_index)
        except TypeError:
            logger.warning("Cannot add index, is %s", table_index)
        seg_specifics["columns"] = list(seg_set)
        seg_specifics["base_meta"] = meta
        table_dispatch_info.append(seg_specifics)
    return table_dispatch_info


def generate_dispatch_info(uuid, env):
    """Generate dispatch info for all batch jobs to run

    Args:
        uuid (str): case uuid
        env (str): name of sumo env to read from

    Returns:
        list: list of all table combinations
    """
    sumo = init_sumo_env(env)
    its_names_and_tags = collect_case_table_name_and_tag(uuid, sumo)
    dispatch_info = []
    for iter_name, table_info in its_names_and_tags.items():
        for table_name, tags in table_info.items():
            for tag_name in tags:
                table_identifier = (uuid, table_name, tag_name, iter_name)

                dispatch_info.extend(
                    generate_dispatch_info_per_combination(table_identifier, sumo)
                )
    return dispatch_info


def aggregate_and_upload(dispatch_info, sumo):
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
