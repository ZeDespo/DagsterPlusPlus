from pathlib import Path

import dagster as dag

from dagster_plus_plus.assets import (
    hello,
    world,
)
from dagster_plus_plus.core.definitions import core_definitions
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
        assets=[hello, world],
        resources={
            "duckdb_conn": DuckDbResource(),
        },
    )
    return dag.Definitions.merge(core_definitions(), pythonic_defs)
