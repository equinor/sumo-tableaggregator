from fmu.sumo.explorer import Explorer
import duckdb
import seaborn as sns
from matplotlib import pyplot as plt
import matplotlib

# matplotlib.use("TkAgg")


def list_as_string(lst):
    return ",".join(['"' + elt + '"' for elt in lst])


explorer = Explorer(env="preview")

caseuuid = "9c9d9a52-1cf4-44cc-829f-23b8334ae813"

case = explorer.get_case_by_uuid(caseuuid)

tables = case.tables
print(f"Names:    {tables.names}")
print(f"Tagnames: {tables.tagnames}")

realizations = list(
    case.tables.filter(name="TROLL", tagname="summary", iteration="iter-0")
)

uuids = [r.uuid for r in realizations if r.realization is not None]
realids = [r.realization for r in realizations if r.realization is not None]
authres = explorer._sumo.get(f"/objects('{caseuuid}')/authtoken").json()
baseuri, auth = authres["baseuri"], authres["auth"]

bloburls = [f"{baseuri}{uuid}?{auth}" for uuid in uuids]

duckdb.sql("create table realfiles (filename VARCHAR, REAL INTEGER)")

for fn, realid in zip(bloburls, realids):
    duckdb.sql(f"INSERT INTO realfiles VALUES ('{fn}', {realid})")

print(duckdb.sql("select * from realfiles").df())

firstobj = explorer._sumo.get(f"/objects('{uuids[0]}')").json()

# columns = firstobj.get("_source").get("data").get("spec").get("columns")

cols = ["DATE", "FOPT", "REAL"]

print(cols)

# print([c for c in columns if c == "REAL"])

res = duckdb.sql(
    f"SELECT {list_as_string(cols)} from read_parquet({bloburls}, filename=T Nrue) JOIN realfiles USING (filename)"
).arrow()

print(len(res.column_names))
print(res.shape)

# sns.lineplot(data=res, x="DATE", y="FOPR", hue="REAL")
# plt.show()
