"""To pick up files from some local filepath."""

from pathlib import Path

import dagster as dag
from pydantic import Field

_DEFAULT_PATH = Path("/opt/filesystem_mount")


class LocalFileDirectoryResource(dag.ConfigurableResource):  # type: ignore[misc]
    """
    Resource that will be used exclusively with a very specific folder. We do not
    want to have our documents to be arbitrarily bindmounted in specific ways.
    """

    ingress_path: str = Field(default=str(_DEFAULT_PATH))

    def create_resource(self, _) -> Path:
        """Return the directory all local files should be ingested from."""
        p = Path(self.ingress_path)
        if not p.exists():
            raise dag.Failure(f"The ingress path {self.ingress_path} does not exist!")
        return p
