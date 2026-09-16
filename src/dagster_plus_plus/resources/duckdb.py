"""DuckDB connectivity resources."""

import dagster as dag
import duckdb
from pydantic import Field, PrivateAttr
from returns.io import IOResultE, impure_safe

from dagster_plus_plus.core.data_store import (
    BaseCache,
)


class DuckDbResource(dag.ConfigurableResource):
    """Allows connection to some duckdb resource."""

    database: str = Field(default=":memory:")
    read_only: bool = Field(default=False)

    def create_resource(self, _) -> duckdb.DuckDBPyConnection:
        """Make the thing."""
        return duckdb.connect(database=self.database, read_only=self.read_only)


class DuckDbCacheResource(dag.ConfigurableResource, BaseCache):
    """An unlogged, temporary table to act as a cache across dagster runs."""

    connection: dag.ResourceDependency[duckdb.DuckDBPyConnection]
    _table_name: str = PrivateAttr(default="Cache")

    def pop(self, key: str) -> IOResultE[str]:
        """Read, delete, then return the value."""
        result = self.read(key)
        result.map(
            lambda _: self.connection.sql(
                f"DELETE FROM {self._table_name} WHERE key = '{key}'"
            )
        )
        return result

    @impure_safe
    def read(self, key: str) -> str:
        """Get cache by key value"""
        result = self.connection.sql(
            f"SELECT value FROM {self._table_name} WHERE key = '{key}'"
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
        )

    def write(self, key: str, value: str):
        """Just writes to the cache. If a key already exists, update it."""
        self.connection.sql(
            f"""
            INSERT OR REPLACE INTO {self._table_name} (key, value)
            VALUES ('{key}', '{value}');
            """
        )
