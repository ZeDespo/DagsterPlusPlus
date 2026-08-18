"""
Holds the core
"""

import dagster as dag

from dagster_plus_plus.core.resources.io_manager import DagPlusPlusIOManager
from dagster_plus_plus.core.resources.postgres import (
    PostgresCache,
    PostgresIOManagerDataStoreResource,
    PostgresqlResource,
)


def core_definitions(
    executor: dag.Executor | dag.ExecutorDefinition | None = None,
) -> dag.Definitions:
    """These are the definitions that SIP needs to run."""
    postgres_resource = PostgresqlResource(
        host=dag.EnvVar("DAGSTER_POSTGRES_HOST"),
        username=dag.EnvVar("DAGSTER_POSTGRES_USER"),
        password=dag.EnvVar("DAGSTER_POSTGRES_PASSWORD"),
    )
    data_store = PostgresIOManagerDataStoreResource(postgres=postgres_resource)
    return dag.Definitions(
        resources={
            "io_data_store": data_store,
            "io_manager": DagPlusPlusIOManager(data_store=data_store),
            "cache": PostgresCache(postgres=postgres_resource),
        },
        executor=executor,
    )
