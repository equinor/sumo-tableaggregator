"""Dispatches jobs to to radix"""

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
    names_and_tags = {}
    iterations = ut.query_sumo_iterations(sumo, uuid)
    for iter_name in iterations:
        names_and_tags[iter_name] = ut.query_for_name_and_tags(sumo, uuid, iter_name)
    return names_and_tags


def dispath_info(uuid, name, tag, iteration, sumo, segment_length, **kwargs):
    logger = ut.init_logging(__file__ + ".dispatch_info")
    (
        object_ids,
        meta,
        table_index,
    ) = ut.query_for_table(sumo, uuid, name, tag, iteration, **kwargs)
    table_index = meta["data"]["spec"]["table_index"]
    segments = ut.split_list(meta["data"]["spec"]["columns"], segment_length)
    segs_w_table_index = []
    for segment in segments:
        seg_set = set(segment)
        try:
            seg_set.update(table_index)
        except TypeError:
            logger.warning("Cannot add index, is %s", table_index)
        segs_w_table_index.append(tuple(seg_set))
    return tuple(segs_w_table_index)
