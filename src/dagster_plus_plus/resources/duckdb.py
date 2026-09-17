"""DuckDB connectivity resources."""

from typing import Any

import dagster as dag
import duckdb
from pydantic import Field, PrivateAttr
from returns.io import IOResultE, impure_safe

from dagster_plus_plus.core.data_store import (
    BaseCache,
    BaseIODataStore,
    IOKey,
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


class DuckDbIOManagerDataStoreResource(dag.ConfigurableResource, BaseIODataStore):
    """To be used exclusively with an IO manager."""

    connection: dag.ResourceDependency[duckdb.DuckDBPyConnection]
    _table_name: str = PrivateAttr(default="DagsterIO")

    @impure_safe
    def read(self, key: IOKey) -> str:
        """Get value by primary key"""
        dag.get_dagster_logger().info(
            self.connection.sql(f"SELECT * FROM {self._table_name}")
        )
        return self.connection.sql(
            f"""
            SELECT encoded_output
            FROM {self._table_name}
            WHERE
                upstream_name = {key.upstream_name!r} AND
                partition_key = {key.partition_key!r} AND
                dynamic_output_mapping_key = {key.dynamic_output_mapping_key!r} AND
                op_output_name = {key.op_output_name!r}
            """
        ).fetchone()[0]

    @impure_safe
    def read_metadata(self, key: IOKey) -> dict[str, Any]:
        """Grab the metadata for an output object."""
        return self.connection.sql(
            f"""
            SELECT metadata
            FROM {self._table_name}
            WHERE
                upstream_name = '{key.upstream_name}' AND
                partition_key = '{key.partition_key}' AND
                dynamic_output_mapping_key = '{key.dynamic_output_mapping_key}' AND
                op_output_name = '{key.op_output_name}';
            """
        ).fetchone()[0]

    def setup_for_execution(self, _) -> None:
        """Create the table for Dagster IO"""
        self.connection.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self._table_name}
            (
                upstream_name VARCHAR,
                partition_key VARCHAR,
                dynamic_output_mapping_key VARCHAR,
                op_output_name VARCHAR,
                encoded_output VARCHAR,
                metadata JSON,
                PRIMARY KEY (
                    upstream_name,
                    partition_key,
                    dynamic_output_mapping_key,
                    op_output_name
                )
            );
            """
        )

    def write(
        self, key: IOKey, value: str, metadata: dict[str, Any] | None = None
    ) -> None:
        """Write to the db"""
        self.connection.sql(
            f"""
            INSERT INTO {self._table_name} (
                upstream_name,
                partition_key,
                dynamic_output_mapping_key,
                op_output_name,
                encoded_output,
                metadata
            ) VALUES (
                '{key.upstream_name}',
                '{key.partition_key}',
                '{key.dynamic_output_mapping_key}',
                '{key.op_output_name}',
                '{value}',
                '{metadata or {}}'
            )
            ON CONFLICT DO UPDATE SET
                encoded_output = EXCLUDED.encoded_output,
                metadata = EXCLUDED.metadata
            """
        )
