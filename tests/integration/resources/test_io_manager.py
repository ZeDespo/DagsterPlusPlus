"""
Test by running dagster jobs.
"""

import dagster as dag
import pytest
from faker import Faker

from dagster_plus_plus.core.data_store import BaseIODataStore
from dagster_plus_plus.resources.io_manager import DagPlusPlusIOManager
from dagster_plus_plus.resources.postgres import (
    PostgresIOManagerDataStoreResource,
    PostgresqlResource,
)

_COLORS = ["red", "green", "blue"]
_STATIC_PARTITION = dag.StaticPartitionsDefinition(_COLORS)
_DYNAMIC_PARTITION = dag.DynamicPartitionsDefinition(name="dyn")
_MULTI_DIM_PARTITION = dag.MultiPartitionsDefinition(
    partitions_defs={
        "color": _STATIC_PARTITION,
        "dyn": _DYNAMIC_PARTITION,
    }
)


@pytest.fixture(params=[None, _STATIC_PARTITION])
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
    standard_pk = faker.random_choices(_COLORS, length=1)[0]
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
    @pytest.fixture(params=[0])
    def io_data_store(
        self, request: pytest.FixtureRequest, postgres_resource: PostgresqlResource
    ) -> BaseIODataStore:
        data_store_mapping = {
            0: PostgresIOManagerDataStoreResource(postgres=postgres_resource)
        }
        io_ds = data_store_mapping[request.param]
        return io_ds

    @pytest.fixture(params=[0])
    def defs(self, io_data_store: BaseIODataStore) -> dag.Definitions:
        return dag.Definitions(
            resources={"io_manager": DagPlusPlusIOManager(io_data_store=io_data_store)}
        )

    @pytest.fixture(autouse=True)
    def setup(self, io_data_store):
        yield
        # DagsterIOManagement.truncate_table()

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
