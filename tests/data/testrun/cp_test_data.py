import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.feather as pf
from pathlib import Path

CASE_PATH = Path("/scratch/fmu/dbs/drogon_design-2022-12-01/")

RELEVANT_PATHS = CASE_PATH.glob(
    "realization-*/iter-0/share/results/tables/.summary.arrow.yml"
)

for rel_path in RELEVANT_PATHS:
    # date_col = "DATE"
    # dframe = pf.read_table(rel_path).to_pandas().head()
    # dframe.index = pd.to_datetime(dframe.index, unit="ms")
    # dframe = dframe.astype(np.float32)

    # schema = [pa.field(date_col, pa.timestamp("ms"))]
    # schema.extend(
    # [pa.field(col, pa.float32()) for col in dframe.columns if col != date_col]
    # )
    cp_path = Path(str(rel_path).replace(str(CASE_PATH) + "/", ""))
    # print(cp_path.parent)
    # Path(cp_path.parent).mkdir(parents=True, exist_ok=True)
    # # print(p)
    # # exit()
    # table = pa.Table.from_pandas(dframe, schema=pa.schema(schema))
    # print(table)
    cp_path.write_text(rel_path.read_text())
    print(cp_path)
    # pf.write_feather(table, cp_path)
