"""DuckDB connectivity resources."""

import dagster as dag
import duckdb
from pydantic import Field, PrivateAttr
from returns.result import safe

from dagster_plus_plus.core.data_store import (
    BasicDataStore,
)


class DuckDbResource(dag.ConfigurableResource):
    """Allows connection to some duckdb resource."""

    path: str = Field(default=":memory:")
    read_only: bool = Field(default=False)

    def create_resource(self, _) -> duckdb.DuckDBPyConnection:
        """Make the thing."""
        return duckdb.connect(database=self.path, read_only=self.read_only)


class DuckDbCache(dag.ConfigurableResource, BasicDataStore):
    """A connection to some in-memory duckdb instance to act
    as a cache"""

    connection: dag.ResourceDependency[duckdb.DuckDBPyConnection]
    _table_name: str = PrivateAttr(default="Cache")

    @safe
    def read(self, key: str) -> str:
        """Get cache by key value"""
        result = self.connection.sql(
            f"SELECT value FROM {self._table_name} WHERE key = {key}"
        ).fetchone()
        if result:
            return result[0]
        raise KeyError(f"Duckdb cache key {key!r} not set.")

    def setup_for_execution(self, _) -> None:
        """
        Create the tables if they do not already exist.
        """
        self.connection.sql(
            f"""
            CREATE TEMP TABLE IF NOT EXISTS {self._table_name}
            (key VARCHAR PRIMARY KEY, value VARCHAR);
            """
        ).execute()

    def write(self, key: str, value: str):
        """Just writes to the cache."""
        self.connection.sql(
            f"INSERT INTO {self._table_name} VALUES ({key}), ({value});"
        ).execute()
