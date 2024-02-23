import duckdb
import logging
from test_setup import make_bloburls_and_cols, list_as_string, evaluate_reading_many

logging.basicConfig(level="INFO")
logger = logging.getLogger("duckdb")

bloburls, realids, cols = make_bloburls_and_cols("DROGON")

duckdb.sql("set threads = 40")
duckdb.sql("set memory_limit = '32GiB'")
duckdb.sql("set preserve_insertion_order = false")

duckdb.sql("create table realfiles (filename VARCHAR, REAL INTEGER)")

for fn, realid in zip(bloburls, realids):
    duckdb.sql(f"INSERT INTO realfiles VALUES ('{fn}', {realid})")


def read_with_duckdb(columns, urls):

    return duckdb.sql(
        f"SELECT {list_as_string(columns)} from read_parquet({urls}, filename=True) JOIN realfiles USING (filename)"
    ).arrow()


evaluate_reading_many(read_with_duckdb, cols, bloburls)
