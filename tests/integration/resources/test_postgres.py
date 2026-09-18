from unittest.mock import Mock

import pytest
from playhouse.postgres_ext import PostgresqlExtDatabase
from returns.io import IOSuccess

from dagster_plus_plus.core.postgres_models import Cache
from dagster_plus_plus.resources.postgres import (
    PostgresCacheResource,
    PostgresqlResource,
)


class TestPostgresqlResource:
    def test_create_resource(self, postgres_resource: PostgresqlResource):
        assert isinstance(
            postgres_resource.create_resource(Mock()), PostgresqlExtDatabase
        )


class TestPostgresCacheResource:
    @pytest.fixture
    def cache(self, postgres_resource: PostgresqlResource):
        cache = PostgresCacheResource(
            postgres=postgres_resource.create_resource(Mock())
        )
        cache.setup_for_execution(Mock())
        yield cache
        Cache.truncate_table()

    def test_setup_for_execution(self, cache: PostgresCacheResource):
        """Will fail if cache table is not set up"""
        assert isinstance(list(Cache.select().execute()), list)

    def test_pop(self, cache: PostgresCacheResource):
        Cache.create(key="a", value="b")
        result = cache.pop("a")
        assert result == IOSuccess("b")
        assert Cache.get_or_none(key="a") is None

    def test_pop_invalid(self, cache: PostgresCacheResource):
        assert cache.pop("a").failure()

    def test_read(self, cache: PostgresCacheResource):
        Cache.create(key="a", value="b")
        assert cache.read("a") == IOSuccess("b")
        assert Cache.get_or_none(key="a")

    def test_read_invalid(self, cache: PostgresCacheResource):
        assert cache.read("a").failure()

    def test_write(self, cache: PostgresCacheResource):
        cache.write("a", "b")
        assert Cache.get(key="a").value == "b"
        cache.write("a", "c")
        assert Cache.get(key="a").value == "c"
