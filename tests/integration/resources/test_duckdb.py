from pathlib import Path
from unittest.mock import Mock

import duckdb
import pytest
from returns.io import IOSuccess

from dagster_plus_plus.resources.duckdb import DuckDbCacheResource, DuckDbResource


@pytest.fixture(params=[":memory:"])
def database(request: pytest.FixtureRequest):
    """Cleanup rogue file if pass in a path parameter"""
    database = request.param
    yield str(database)
    if isinstance(database, Path):
        database.unlink()


class TestDuckDbResource:
    @pytest.mark.parametrize(
        "database",
        [":memory:", Path(__file__).parent.resolve() / "duck.db"],
        indirect=True,
    )
    def test_create_resource(self, database: str):
        duckdb_resource = DuckDbResource(
            database=database, read_only=False
        ).create_resource(Mock())
        assert isinstance(duckdb_resource, duckdb.DuckDBPyConnection)
        if database != ":memory:":
            assert Path(database).exists()


class TestDuckDbCacheResource:
    @pytest.fixture
    def connection(self, database: str) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(database=database)

    @pytest.fixture
    def cache(self, connection: duckdb.DuckDBPyConnection) -> DuckDbCacheResource:
        return DuckDbCacheResource(connection=connection)

    @pytest.fixture(autouse=True)
    def setup(self, cache: DuckDbCacheResource):
        cache.setup_for_execution(Mock())

    def test_setup_for_execution(self, cache: DuckDbCacheResource):
        assert isinstance(
            cache.connection.sql(f"SELECT * FROM {cache._table_name};").fetchall(), list
        )

    def test_read(self, cache: DuckDbCacheResource):
        cache.connection.sql(
            f"INSERT INTO {cache._table_name} (key, value) VALUES ('a', 'b');"
        )
        assert cache.read("a") == IOSuccess("b")

    def test_read_non_existent_key(self, cache: DuckDbCacheResource):
        assert cache.read("a").failure()

    def test_pop(self, cache: DuckDbCacheResource):
        cache.connection.sql(
            f"INSERT INTO {cache._table_name} (key, value) VALUES ('a', 'b');"
        )
        result = cache.pop("a")
        assert result == IOSuccess("b")
        assert not cache.connection.sql(
            f"SELECT * FROM {cache._table_name};"
        ).fetchall()

    def test_pop_non_existent_key(self, cache: DuckDbCacheResource):
        assert cache.pop("a").failure()

    def test_write(self, cache: DuckDbCacheResource):
        cache.write("a", "b")
        assert cache.read("a") == IOSuccess("b")
        cache.write("a", "c")
        assert cache.read("a") == IOSuccess("c")
