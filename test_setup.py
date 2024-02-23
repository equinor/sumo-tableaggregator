import os
from sumo.wrapper import SumoClient
import logging
from datetime import datetime, timedelta


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


logging.basicConfig(level="DEBUG")
logger = logging.getLogger("duckdb")


CASE_UUID = "9c9d9a52-1cf4-44cc-829f-23b8334ae813"

CLIENT = SumoClient(env="preview")
logger.info(f"PID: {os.getpid()}")


def convert_seconds(seconds):
    sec = timedelta(seconds=seconds)
    print(sec)
    d = datetime(1, 1, 1) + sec
    print(d)
    return f"{d.minute} minutes, {d.second} seconds"


def make_bloburls_and_cols():
    real_dict = query_for_uuids_and_reals(
        CASE_UUID, "TROLL", "summary", "iter-0", CLIENT
    )
    print(real_dict)

    uuids = list(real_dict.values())
    realizations = list(real_dict.keys())
    print(len(uuids))
    print(len(realizations))

    firstobj = CLIENT.get(f"/objects('{uuids[0]}')").json()

    columns = firstobj.get("_source").get("data").get("spec").get("columns")
    print(len(columns))

    cols = columns[:10]

    authres = CLIENT.get(f"/objects('{CASE_UUID}')/authtoken").json()
    baseuri, auth = authres["baseuri"], authres["auth"]

    bloburls = [f"{baseuri}{uuid}?{auth}" for uuid in uuids]
    return bloburls, realizations, cols


def digest(start, stop, res):
    elapsed = stop - start
    logger.info("Elapsed: %s ", f"{elapsed: 5.2f} seconds ({convert_seconds(elapsed)})")

    logger.debug(len(res.column_names))
    logger.debug(res.shape)


def main():
    print(convert_seconds(1))


if __name__ == "__main__":
    main()
