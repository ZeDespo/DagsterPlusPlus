from pathlib import Path

import dagster as dag
import duckdb
import pandas as pd

_PATH_TO_RAW_DATA_FILES = Path(__file__).parent / "raw_data"


def _create_table(
    context: dag.AssetExecutionContext, duckdb_conn: duckdb.DuckDBPyConnection, key: str
):
    df = pd.read_csv(_PATH_TO_RAW_DATA_FILES / f"raw_{key}.csv")
    context.log.debug(df.head())
    duckdb_conn.sql("CREATE SCHEMA IF NOT EXISTS raw")
    duckdb_conn.sql(f"CREATE OR REPLACE TABLE raw.raw_{key} AS SELECT * FROM df")
    context.add_output_metadata({"n_rows": df.shape[0]})


@dag.asset
def raw_customers_loaded(
    context: dag.AssetExecutionContext,
    duckdb_conn: dag.ResourceParam[duckdb.DuckDBPyConnection],
):
    return _create_table(context, duckdb_conn, "customers")


@dag.asset
def raw_orders_loaded(
    context: dag.AssetExecutionContext,
    duckdb_conn: dag.ResourceParam[duckdb.DuckDBPyConnection],
):
    return _create_table(context, duckdb_conn, "orders")


@dag.asset
def hello():
    return "hello"


@dag.asset
def world(hello: str):
    return f"{hello} world!"
