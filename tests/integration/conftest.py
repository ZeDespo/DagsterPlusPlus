"""
Dagster needs certain fixtures to run.
"""

import os

import dagster as dag
import pytest

from dagster_plus_plus.resources.duckdb import DuckDbResource
from dagster_plus_plus.resources.postgres import (
    PostgresqlResource,
)


@pytest.fixture
def instance() -> dag.DagsterInstance:
    return dag.DagsterInstance.ephemeral()


@pytest.fixture
def duckdb_resource() -> DuckDbResource:
    return DuckDbResource(database="dagplusplus.db", read_only=False)


@pytest.fixture
def postgres_resource() -> PostgresqlResource:
    return PostgresqlResource(
        host="localhost",
        port=5432,
        username=os.environ["DAGSTER_POSTGRES_USER"],
        password=os.environ["DAGSTER_POSTGRES_PASSWORD"],
        db_name=os.environ["DAGSTER_POSTGRES_NAME"],
    )
