from sumo.wrapper import SumoClient
import polars as pl
import time


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


caseuuid = "9c9d9a52-1cf4-44cc-829f-23b8334ae813"

client = SumoClient(env="preview")

real_dict = query_for_uuids_and_reals(caseuuid, "TROLL", "summary", "iter-0", client)
print(real_dict)

uuids = list(real_dict.values())
realizations = list(real_dict.keys())
print(len(uuids))
print(len(realizations))
# uuids = [r.uuid for r in realizations if r.realization is not None]
# # for uuid in uuids:

# #     blob_url = explorer._sumo.get(f"/objects('{uuid}')/blob/authuri?readwrite=w").text


firstobj = client.get(f"/objects('{uuids[0]}')").json()

columns = firstobj.get("_source").get("data").get("spec").get("columns")
print(len(columns))

cols = columns[:10]

# print(cols)

# print([c for c in columns if c == "REAL"])

authres = client.get(f"/objects('{caseuuid}')/authtoken").json()
baseuri, auth = authres["baseuri"], authres["auth"]

bloburls = [f"{baseuri}{uuid}?{auth}" for uuid in uuids]

t0 = time.perf_counter()
df = pl.scan_parquet(bloburls).select(columns)

sql = pl.SQLContext(my_table=df)

query = "SELECT * from my_table"

res = sql.execute(query, eager=True)

print(res.shape)
t1 = time.perf_counter()
print(f"Elapsed: {t1 - t0:5.2f} seconds.")
