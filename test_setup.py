import os
from sumo.wrapper import SumoClient
import logging
from datetime import datetime, timedelta
import time


TEST_CASES = {
    "DROGON": {"case_uuid": "ac1a2df9-961e-4a54-82a6-7da7283a2942", "env": "dev"},
    "TROLL": {"case_uuid": "9c9d9a52-1cf4-44cc-829f-23b8334ae813", "env": "preview"},
}

ROUNDS = 10


def query_for_uuids_and_reals(case_uuid, name, tagname, iterationname, sumo):
    """Query for what we need to fetching data

    Args:
        case_uuid (str): uuid for case
        name (str): name of table
        tagname (str): name of tag
        iterationname (str): name of iteration
        sumo (SumoClient): client to use for query

    Returns:
        dict: keys are
    """
    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"fmu.case.uuid.keyword": {"value": case_uuid}}},
                    {"term": {"data.name.keyword": {"value": name}}},
                    {"term": {"data.tagname.keyword": {"value": tagname}}},
                    {"term": {"fmu.iteration.name.keyword": {"value": iterationname}}},
                    {"term": {"fmu.context.stage.keyword": {"value": "realization"}}},
                ]
            }
        },
        "size": 500,
        "_source": {"includes": ["fmu.realization.id", "_id"]},
    }
    query_results = sumo.post("/search", json=query).json()
    blob_ids = {}
    for hit in query_results["hits"]["hits"]:
        blob_ids[hit["_source"]["fmu"]["realization"]["id"]] = hit["_id"]
    return blob_ids


def list_as_string(lst):
    return ",".join(['"' + el + '"' for el in lst])


def Average(lst):
    return sum(lst) / len(lst)


logger = logging.getLogger("setup")
logger.info(f"PID: {os.getpid()}")


def convert_seconds(seconds):
    sec = timedelta(seconds=seconds)
    d = datetime(1, 1, 1) + sec
    return f"{d.minute} minutes, {d.second} seconds"


def make_bloburls_and_cols(asset_name):
    uuid = TEST_CASES[asset_name]["case_uuid"]
    client = SumoClient(env=TEST_CASES[asset_name]["env"])
    real_dict = query_for_uuids_and_reals(uuid, asset_name, "summary", "iter-0", client)
    logger.debug(real_dict)

    uuids = list(real_dict.values())
    realizations = list(real_dict.keys())
    logger.debug(len(uuids))
    logger.debug(len(realizations))

    firstobj = client.get(f"/objects('{uuids[0]}')").json()

    columns = firstobj.get("_source").get("data").get("spec").get("columns")
    logger.debug(len(columns))

    cols = columns[:10]

    authres = client.get(f"/objects('{uuid}')/authtoken").json()
    baseuri, auth = authres["baseuri"], authres["auth"]

    bloburls = [f"{baseuri}{uuid}?{auth}" for uuid in uuids]
    return bloburls, realizations, cols


def evaluate_reading(func, *args):
    time.sleep(1)
    t0 = time.perf_counter()
    res = func(*args)
    t1 = time.perf_counter()
    time.sleep(1)
    digest(t0, t1, res)
    return t1 - t0


def evaluate_reading_many(func, *args):

    timings = []
    for _ in range(ROUNDS):
        timings.append(evaluate_reading(func, *args))

    print(f"Elapsed mean: {Average(timings):5.2f} seconds.")
    print(f"{min(timings): 5.2f} - {max(timings): 5.2f}")


def digest(start, stop, res):
    elapsed = stop - start
    logger.info("Elapsed: %s ", f"{elapsed: 5.2f} seconds ({convert_seconds(elapsed)})")
    logger.debug(len(res.column_names))
    logger.debug(res.shape)
