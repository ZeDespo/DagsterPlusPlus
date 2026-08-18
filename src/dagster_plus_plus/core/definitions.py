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
    """These are the definitions that DagPlusPlus needs to run."""
    postgres = PostgresqlResource(
        host=dag.EnvVar("DAGSTER_POSTGRES_HOST"),
        username=dag.EnvVar("DAGSTER_POSTGRES_USER"),
        password=dag.EnvVar("DAGSTER_POSTGRES_PASSWORD"),
    )
    return dag.Definitions(
        resources={
            "io_manager": DagPlusPlusIOManager(
                io_data_store=PostgresIOManagerDataStoreResource(postgres=postgres)
            ),
            "cache": PostgresCache(postgres=postgres),
        },
        executor=executor,
    )
