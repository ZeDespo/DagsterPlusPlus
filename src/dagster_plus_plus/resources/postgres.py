"""
Postgresql connectivity resources.
"""

from typing import Any

import dagster as dag
import peewee
from playhouse.postgres_ext import PostgresqlExtDatabase
from pydantic import Field
from returns.io import IOResultE, impure_safe
from returns.result import safe

from dagster_plus_plus.core.data_store import (
    BaseCache,
    BaseIODataStore,
    IOKey,
)
from dagster_plus_plus.core.postgres_models import (
    Cache,
    DagsterIOManagement,
)


class PostgresqlResource(dag.ConfigurableResource):
    """
    The bare-basic peewee connection to pass onto peewee models.

    You should never use this resource directly. Define your models in your
    client's postgresql resource, and use this one as a dependency.
    """

    host: str = Field()
    username: str = Field()
    password: str = Field()
    port: int = Field(default=5432)
    db_name: str = Field(default="dagster")

    def create_resource(self, _) -> PostgresqlExtDatabase:
        """Creates the resource as an object."""
        return PostgresqlExtDatabase(
            self.db_name,
            user=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
        )


class PostgresCacheResource(dag.ConfigurableResource, BaseCache):
    """An unlogged table to act as a redis cache."""

    postgres: dag.ResourceDependency[PostgresqlExtDatabase]

    def pop(self, key: str) -> IOResultE[Any]:
        """Read the value then pop the key from the unlogged table."""
        result = self.read(key)
        result.map(lambda _: Cache.delete_by_id(key))
        return result

    @impure_safe
    def read(self, key: str) -> str:
        """Get cache by key value"""
        return Cache.get(key=key).value

    def setup_for_execution(self, _) -> None:
        """
        Create the tables if they do not already exist.
        """
        self.postgres.bind(models=[Cache])
        try:
            self.postgres.execute_sql(
                """
                CREATE UNLOGGED TABLE cache (
                    key VARCHAR(255) PRIMARY KEY,
                    value VARCHAR(1024) NOT NULL
                );
                """
            )
        except peewee.ProgrammingError as e:
            if f'relation "{Cache.__name__.lower()}" already exists' not in str(e):
                raise

    def write(self, key: str, value: str):
        """Just writes to the cache."""
        Cache.insert(key=key, value=value).on_conflict(
            conflict_target=[Cache.key], preserve=[Cache.value]
        ).execute()


class PostgresIOManagerDataStoreResource(dag.ConfigurableResource, BaseIODataStore):
    """To be used exclusively with the I/O manager"""

    postgres: dag.ResourceDependency[PostgresqlResource]

    @safe
    def read(self, key: IOKey) -> str:
        """Get value by primary key."""
        row = DagsterIOManagement.get(
            upstream_name=key.upstream_name,
            partition_key=key.partition_key,
            dynamic_output_mapping_key=key.dynamic_output_mapping_key,
            op_output_name=key.op_output_name,
        )
        return str(row.encoded_output)

    def read_metadata(self, key: IOKey) -> dict[str, Any]:
        """Get the metadata, if any has been added in for the run."""
        row = DagsterIOManagement.get(
            upstream_name=key.upstream_name,
            partition_key=key.partition_key,
            dynamic_output_mapping_key=key.dynamic_output_mapping_key,
            op_output_name=key.op_output_name,
        )
        return row.metadata

    def setup_for_execution(self, _) -> None:
        """
        Create the tables if they do not already exist.
        """
        self.postgres.bind(models=[DagsterIOManagement])
        DagsterIOManagement.create_table()

    def write(
        self, key: IOKey, value: str, metadata: dict[str, Any] | None = None
    ) -> None:
        """Write the row to the database."""
        DagsterIOManagement.get_or_create(
            upstream_name=key.upstream_name,
            partition_key=key.partition_key,
            dynamic_output_mapping_key=key.dynamic_output_mapping_key,
            op_output_name=key.op_output_name,
            encoded_output=value,
            metadata=metadata,
        ).on_conflict(
            conflict_target=[
                DagsterIOManagement.upstream_name,
                DagsterIOManagement.partition_key,
                DagsterIOManagement.dynamic_output_mapping_key,
                DagsterIOManagement.op_output_name,
            ],
            preserve=[DagsterIOManagement.encoded_output, DagsterIOManagement.metadata],
        )
