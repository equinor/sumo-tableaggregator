import polars as pl
import time
import logging
from test_setup import make_bloburls_and_cols, digest


logging.basicConfig(level="INFO")
logger = logging.getLogger("polars")

bloburls, _, cols = make_bloburls_and_cols()
t0 = time.perf_counter()
df = pl.scan_parquet(bloburls).select(cols)

sql = pl.SQLContext(my_table=df)

query = "SELECT * from my_table"

res = sql.execute(query, eager=True)
t1 = time.perf_counter()
digest(t0, t1, res)
