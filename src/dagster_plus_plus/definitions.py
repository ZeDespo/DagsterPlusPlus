from pathlib import Path

import dagster as dag

from dagster_plus_plus.assets import raw_customers_loaded, raw_orders_loaded
from dagster_plus_plus.core.resources.duckdb import DuckDbResource

# @dag.definitions
# def defs():
#     return dag.load_from_defs_folder(path_within_project=Path(__file__).parent)


@dag.definitions
def defs():
    """Combine all componenets with the core library"""
    component_defs = dag.load_from_defs_folder(
        path_within_project=Path(__file__).parent
    )
    pythonic_defs = dag.Definitions(
        assets=[raw_customers_loaded, raw_orders_loaded],
        resources={
            "duckdb_conn": DuckDbResource(),
        },
    )
    return dag.Definitions.merge(component_defs, pythonic_defs)
