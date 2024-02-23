import polars as pl
import logging
from test_setup import make_bloburls_and_cols, evaluate_reading


logging.basicConfig(level="INFO")
logger = logging.getLogger("polars")

bloburls, _, cols = make_bloburls_and_cols("DROGON")


def read_with_polars(columns, urls):

    df = pl.scan_parquet(urls).select(columns)

    sql = pl.SQLContext(my_table=df)

    query = "SELECT * from my_table"

    return sql.execute(query, eager=True).to_arrow()


evaluate_reading(read_with_polars, cols, bloburls)
