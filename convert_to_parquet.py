from fmu.sumo.explorer import Explorer
from io import BytesIO
from sumo.table_aggregation.utilities import blob_to_table, table_to_bytes

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
for uuid in uuids:

    blob_url = explorer._sumo.get(f"/objects('{uuid}')/blob/authuri?readwrite=w").text
    try:
        table = blob_to_table(
            BytesIO(explorer._sumo.get(f"/objects('{uuid}')/blob").content)
        )

        bytestring = table_to_bytes(table)
        response = explorer._sumo.blob_client.upload_blob(blob=bytestring, url=blob_url)
        print(response)
    except Exception:
        print("Should be fine")
