from fmu.sumo.explorer import Explorer
import polars as pl


def list_as_string(lst):
    return ",".join(['"' + el + '"' for el in lst])


explorer = Explorer(env="dev")

caseuuid = "ac1a2df9-961e-4a54-82a6-7da7283a2942"

case = explorer.get_case_by_uuid(caseuuid)

tables = case.tables
print(f"Names:    {tables.names}")
print(f"Tagnames: {tables.tagnames}")

realizations = list(case.tables.filter(name="DROGON", tagname="summary"))

uuids = [r.uuid for r in realizations if r.realization is not None]
# for uuid in uuids:

#     blob_url = explorer._sumo.get(f"/objects('{uuid}')/blob/authuri?readwrite=w").text


firstobj = explorer._sumo.get(f"/objects('{uuids[0]}')").json()

columns = firstobj.get("_source").get("data").get("spec").get("columns")

cols = columns[:10]

print(cols)

print([c for c in columns if c == "REAL"])

authres = explorer._sumo.get(f"/objects('{caseuuid}')/authtoken").json()
baseuri, auth = authres["baseuri"], authres["auth"]

bloburls = [f"{baseuri}{uuid}?{auth}" for uuid in uuids]


df = pl.scan_parquet(bloburls).select(columns)

sql = pl.SQLContext(my_table=df)

query = "SELECT * from my_table"

res = sql.execute(query, eager=True)

print(res.shape)
