"""
Template variables to be used for the ``defs.yaml`` file via
jinja templating.
"""

from collections.abc import Callable

import dagster as dag


@dag.template_var
def group_by_stage() -> Callable[[str], str]:
    """
    Given the original file path thanks to the ``node`` templating
    variable, provide the group that each asset for the dbt models
    should
    """

    def inner(original_file_path: str):
        if "staging" in original_file_path:
            return "staging"
        if "raw" in original_file_path:
            return "raw"
        return "default"

    return inner
