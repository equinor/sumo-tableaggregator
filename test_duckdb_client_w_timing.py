import duckdb
import time
import logging
from test_setup import make_bloburls_and_cols, list_as_string, digest

logging.basicConfig(level="INFO")
logger = logging.getLogger("duckdb")

bloburls, realids, cols = make_bloburls_and_cols()

duckdb.sql("set threads = 40")
duckdb.sql("set memory_limit = '32GiB'")
duckdb.sql("set preserve_insertion_order = false")

duckdb.sql("create table realfiles (filename VARCHAR, REAL INTEGER)")

for fn, realid in zip(bloburls, realids):
    duckdb.sql(f"INSERT INTO realfiles VALUES ('{fn}', {realid})")

time.sleep(1)
t0 = time.perf_counter()
res = duckdb.sql(
    f"SELECT {list_as_string(cols)} from read_parquet({bloburls}, filename=True) JOIN realfiles USING (filename)"
).arrow()
t1 = time.perf_counter()
digest(t0, t1, res)
