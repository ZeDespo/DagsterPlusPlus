"""
Test by running dagster jobs.
"""

import time

import dagster as dag
import pytest
from faker import Faker

from dagster_plus_plus.core.data_store import BaseIODataStore, IOKey
from dagster_plus_plus.resources.duckdb import (
    DuckDbIOManagerDataStoreResource,
    DuckDbResource,
)
from dagster_plus_plus.resources.io_manager import DagPlusPlusIOManager
from dagster_plus_plus.resources.postgres import (
    PostgresIOManagerDataStoreResource,
    PostgresqlResource,
)

_STATIC_PARTITION_KEYS = ["red", "green", "blue"]
_STATIC_PARTITION = dag.StaticPartitionsDefinition(_STATIC_PARTITION_KEYS)
_DYNAMIC_PARTITION = dag.DynamicPartitionsDefinition(name="dyn")
_MULTI_DIM_PARTITION = dag.MultiPartitionsDefinition(
    partitions_defs={
        "color": _STATIC_PARTITION,
        "dyn": _DYNAMIC_PARTITION,
    }
)


@pytest.fixture(
    params=[None, _STATIC_PARTITION, _DYNAMIC_PARTITION, _MULTI_DIM_PARTITION]
)
def partitions_def(request: pytest.FixtureRequest):
    return request.param


@pytest.fixture
def partition_key(
    partitions_def: dag.PartitionsDefinition | None,
    instance: dag.DagsterInstance,
    faker: Faker,
) -> str | None:
    """
    The actual values of these partition keys do not matter if used in a fixture. More
    finite control will be given to fixtures that need it.
    """
    standard_pk = faker.random_choices(_STATIC_PARTITION_KEYS, length=1)[0]
    dynamic_pk = faker.word()
    if isinstance(partitions_def, dag.StaticPartitionsDefinition):
        return standard_pk
    if isinstance(partitions_def, dag.DynamicPartitionsDefinition):
        instance.add_dynamic_partitions("dyn", partition_keys=[dynamic_pk])
        return dynamic_pk
    if isinstance(partitions_def, dag.MultiPartitionsDefinition):
        instance.add_dynamic_partitions("dyn", partition_keys=[dynamic_pk])
        return f"{standard_pk}|{dynamic_pk}"
    return None


class TestDagPlusPlusIOManager:
    @pytest.fixture(params=[0, 1])
    def io_data_store(
        self,
        request: pytest.FixtureRequest,
        postgres_resource: PostgresqlResource,
        duckdb_resource: DuckDbResource,
    ) -> BaseIODataStore:
        data_store_mapping = {
            0: PostgresIOManagerDataStoreResource(postgres=postgres_resource),
            1: DuckDbIOManagerDataStoreResource(connection=duckdb_resource),
        }
        return data_store_mapping[request.param]

    @pytest.fixture(params=[0])
    def defs(self, io_data_store: BaseIODataStore) -> dag.Definitions:
        return dag.Definitions(
            resources={"io_manager": DagPlusPlusIOManager(io_data_store=io_data_store)}
        )

    def test_basic_input_output(
        self,
        defs: dag.Definitions,
        partitions_def: dag.PartitionsDefinition | None,
        partition_key: str | None,
        instance: dag.DagsterInstance,
    ):

        @dag.asset(partitions_def=partitions_def)
        def asset_a() -> int:
            return 0

        @dag.asset(partitions_def=partitions_def)
        def asset_b(asset_a: int):
            assert asset_a == 0
            return f"Value = {asset_a}"

        @dag.asset(partitions_def=partitions_def)
        def asset_c(asset_a: int, asset_b: str):
            assert asset_a == 0
            assert asset_b == f"Value = {asset_a}"

        defs = dag.Definitions.merge(
            defs, dag.Definitions(assets=[asset_a, asset_b, asset_c])
        )
        job = defs.get_implicit_global_asset_job_def()
        job.execute_in_process(
            instance=instance,
            asset_selection=[asset_a.key, asset_b.key, asset_c.key],
            partition_key=partition_key,
        )

    def test_outputs_get_updated_in_database(
        self,
        defs: dag.Definitions,
        partitions_def,
        partition_key,
        instance,
    ):

        @dag.asset(partitions_def=partitions_def)
        def current_time():
            return time.time()

        io_key = IOKey("current_time", partition_key=partition_key)
        io_data_store = defs.resources["io_manager"].io_data_store
        defs = dag.Definitions.merge(defs, dag.Definitions(assets=[current_time]))
        job = defs.get_implicit_global_asset_job_def()
        job.execute_in_process(
            instance=instance,
            asset_selection=[current_time.key],
            partition_key=partition_key,
        )
        value_a = io_data_store.read(io_key)
        job.execute_in_process(
            instance=instance,
            asset_selection=[current_time.key],
            partition_key=partition_key,
        )
        value_b = io_data_store.read(io_key)
        assert value_a != value_b

    def test_non_partitioned_asset_can_read_all_partitioned_ones_as_input(
        self, defs: dag.Definitions, instance, faker: Faker
    ):
        @dag.asset(partitions_def=_STATIC_PARTITION)
        def color(context: dag.AssetExecutionContext) -> int:
            return hash(context.partition_key)

        @dag.asset
        def all_colors(color: dict[str, int]):
            assert all(c in color for c in _STATIC_PARTITION_KEYS)
            for k, v in color.items():
                assert v == hash(k)

        defs = dag.Definitions.merge(defs, dag.Definitions(assets=[color, all_colors]))
        job = defs.get_implicit_global_asset_job_def()
        for static_pk in _STATIC_PARTITION_KEYS:
            job.execute_in_process(
                instance=instance,
                asset_selection=[color.key],
                partition_key=static_pk,
            )
        job.execute_in_process(instance=instance, asset_selection=[all_colors.key])

    def test_partitioned_asset_can_load_non_partitioned_as_input(
        self,
        defs: dag.Definitions,
        instance,
        partitions_def,
        partition_key,
    ):
        if not partitions_def:
            pytest.skip(reason="Need a partitioned asset to test this.")

        @dag.asset
        def all_colors() -> str:
            return "ROY G. BIV"

        @dag.asset(partitions_def=partitions_def)
        def color(all_colors: str):
            assert all_colors == "ROY G. BIV"

        defs = dag.Definitions.merge(defs, dag.Definitions(assets=[color, all_colors]))
        job = defs.get_implicit_global_asset_job_def()
        job.execute_in_process(
            instance=instance,
            asset_selection=[all_colors.key],
        )
        job.execute_in_process(
            instance=instance, asset_selection=[color.key], partition_key=partition_key
        )
