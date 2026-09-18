"""
Peewee models that are used by the core internals.
IO managers, caches, etc.
"""

from pathlib import Path
from typing import Self

import dagster as dag
import peewee
import playhouse.postgres_ext
import regex
from playhouse.shortcuts import ThreadSafeDatabaseMetadata

logger = dag.get_dagster_logger()


class DagPlusPlusBaseModel(peewee.Model):
    """
    Base model that all peewee models should inherit, considering the
    multi-threaded nature of Dagster.
    """

    class Meta:
        """
        Since we need to bind at runtime, according to Peewee docs:

        Peewee database connections are thread-safe. However, if you plan to bind
        the database at run-time in a multi-threaded application, storing the
        models database in a thread-local is necessary.
        This can be accomplished with the ThreadSafeDatabaseMetadata.
        """

        model_metadata_class = ThreadSafeDatabaseMetadata


class DagsterIOManagement(DagPlusPlusBaseModel):
    """Holds an asset / op's returned value from the IO Manager."""

    upstream_name = peewee.CharField(max_length=256)
    partition_key = peewee.CharField(max_length=256)
    dynamic_output_mapping_key = peewee.CharField(max_length=64)
    op_output_name = peewee.CharField(max_length=64)
    encoded_output = playhouse.postgres_ext.TextField()
    metadata = playhouse.postgres_ext.BinaryJSONField(default=dict)

    class Meta:
        """Need snake case name and composite indexes."""

        table_name = "dagster_io_management"
        indexes = (
            (
                (
                    "upstream_name",
                    "partition_key",
                    "dynamic_output_mapping_key",
                    "op_output_name",
                ),
                True,  # Unique constraint activated.
            ),
        )


class Cache(DagPlusPlusBaseModel):
    """
    Used primarily to write information that does NOT need to persist between
    shutdowns / restarts.
    """

    key = peewee.CharField(max_length=255, primary_key=True)
    value = peewee.CharField(max_length=1024)


FileKeyGeneratorType = type[Path | str]
"""
These are the supported types to generate the index key.
"""


def _extract_filename_from_object(file: FileKeyGeneratorType) -> str:
    if isinstance(file, Path):
        return file.name
    if isinstance(file, str):
        return file
    raise TypeError(f"Unhandled type {type(file)} to convert to file key")


def _make_file_key(file: FileKeyGeneratorType) -> str:
    filename = _extract_filename_from_object(file)
    filename = filename.replace(Path(filename).suffix, "")
    filename = regex.sub(r"\s{2,}", " ", filename)
    return regex.sub(r"[^a-zA-Z0-9]", "_", filename)


class PipelineFile(DagPlusPlusBaseModel):
    """
    If your pipeline calls for keeping a record of any file that makes it through,
    use this model to hold pertinent information about it.

    You'll rarely look up individual rows this way, but you can use Peewee select
    statements to

    You can either use this model for your use case and use the ``metadata`` column
    as a dumping ground, or you can inherit from this class and add whatever columns
    you want to add that can be easily structured.

    """

    id = peewee.AutoField(primary_key=True)
    original_filename = peewee.CharField()
    file_key = peewee.CharField(max_length=255)
    """A standardized file stem of the original filename."""
    step_name = peewee.CharField(max_length=128)
    """The AssetKey or the op name that initially put this file in the database."""
    partition_key = peewee.CharField(max_length=128, default="")
    """
    Which partitioned asset (if any) is responsible for putting
    this in the pipeline.
    """
    metadata = playhouse.postgres_ext.BinaryJSONField(default=dict)
    """Dump your JSON here if you'd like."""

    class Meta:
        """Simple table names"""

        table_name = "pipeline_documents"
        indexes = (
            (
                ("file_key",),
                True,  # All file keys must be unique.
            ),
            (
                (
                    "file_key",
                    "step_name",
                    "partition_key",
                ),
                False,
            ),
        )

    @classmethod
    def get_or_none(cls, file: FileKeyGeneratorType) -> Self | None:
        """
        Override base method so we do not rely on someone making an arbitrary file key.

        If you know the ``file_key`` value in advance, just use the base peewee
        ``get_or_none`` method instead.
        """
        return super().get_or_none(cls.file_key == _make_file_key(file))

    @classmethod
    def get_or_create_file(
        cls,
        context: dag.AssetExecutionContext | dag.OpExecutionContext,
        file: FileKeyGeneratorType,
    ) -> tuple[Self, bool]:
        """
        If we are unsure of whether the document was added mid-pipeline /
        with pre-existing metadata or not, we want an easy way to reference these things
        and provide answers on whether this document previously existed or not.

        :param file: What to lookup in the database.
        :param file_key_override: If specified, you want to override the default \
        file key generation in favor of using this string. Be careful.
        :return: A tuple of the document and True if it was created, False if not
        """
        if document := cls.get_or_none(file):
            return document, False
        return cls.create(
            file_key=_make_file_key(file),
            original_filename=_extract_filename_from_object(file),
            step_name=context.asset_key.to_python_identifier()
            if isinstance(context, dag.AssetExecutionContext)
            else context.op.name,
            partition_key=context.partition_key,
        ), True

    def update_metadata(self, **new_metadata) -> Self:
        """
        Concatenate the currently existing metadata with what's passed in. In case of
        duplicate keys, the parameters will overwrite what is in the metadata.
        """
        cls = type(self)
        return next(
            iter(
                cls.update(
                    {
                        cls.metadata: cls.metadata.concat(
                            {k: v for k, v in new_metadata.items()}
                        )
                    }
                )
                .where(cls.file_key == self.file_key)
                .returning(cls)
            )
        )
