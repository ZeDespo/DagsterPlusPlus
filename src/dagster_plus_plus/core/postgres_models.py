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


FileKeyGeneratorType: type = Path | str
"""
These are the supported types to generate the index key.
"""


def _extract_filename_from_object(file: FileKeyGeneratorType) -> str:
    if isinstance(file, Path):
        return file.name
    if isinstance(file, str):
        return file
    raise TypeError(f"Unhandled type {type(file)} to convert to file key")


class PipelineDocument(DagPlusPlusBaseModel):
    """
    Holds pertitent,individual document information. Is indexed based off a formatted
    filename, therefore, you cannot hold two

    You can either use this model for your use case and use the ``metadata`` column
    as a dumping ground, or you can inherit from this class and add whatever columns
    you want to add that can be easily structured.

    Would recommend using the ``metadata`` column as a means to dump data that would
    facilitate multiple rows in some table, such as timestamps for when the
    documents go through certain runs. Using metadata as a means to pass
    information across assets leads to the usual headaches of dealing with
    unstructured data (null keys, keys not existing, keys easily getting erased on
    update by mistake).
    """

    id = peewee.AutoField(primary_key=True)
    original_filename = peewee.CharField()
    file_key = peewee.CharField(max_length=255)
    """A standardized file stem of the original filename."""
    partition_key = peewee.CharField(max_length=64, default="")
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
                    "partition_key",
                ),
                False,
            ),
        )

    @classmethod
    def get_document_or_none(cls, file: FileKeyGeneratorType) -> Self | None:
        """
        Return the document if the generated key exists, else return a Nothing object.

        If you know the ``file_key`` value in advance, just use the base peewee
        ``get_or_none`` method instead.
        """
        return cls.get_or_none(
            cls.file_key == cls.make_file_key(file, remove_suffix=True)
        )

    @classmethod
    def get_or_create_document(
        cls,
        context: dag.AssetExecutionContext | dag.OpExecutionContext,
        file: FileKeyGeneratorType,
        file_key_override: str = "",
    ) -> tuple[Self, bool]:
        """
        If we are unsure of whether the document was added mid-pipeline /
        with pre-existing metadata or not, we want an easy way to reference these things
        and provide answers on whether this document previously existed or not.

        We can also use this method to do something incredibly evil: override the base
        logic for generating a ``file_key`` object with our own, by directly passing 
        it in.

        :param file: What to lookup in the database.
        :param file_key_override: If specified, you want to override the default \
        file key generation in favor of using this string. Be careful.
        :return: A tuple of the document and True if it was created, False if not
        """
        file_key = file_key_override or cls.make_file_key(file, remove_suffix=True)
        if document := cls.get_or_none(cls.file_key == file_key):
            return document, False
        return cls.create(
            file_key=file_key,
            original_filename=_extract_filename_from_object(file),
            partition_key=context.partition_key,
        ), True

    @staticmethod
    def make_file_key(file: FileKeyGeneratorType, remove_suffix: bool = True) -> str:
        """
        Creates the primary key for this model. Can use this in your implementation
        to create a base primary key, so you can another row for the same original
        filename.
        """
        filename = _extract_filename_from_object(file)
        if remove_suffix:
            filename = filename.replace(Path(filename).suffix, "")
        return regex.sub(r"[^a-zA-Z0-9]", "_", filename)

    def update_metadata(self, **new_metadata) -> Self:
        """The same exact method as we would update two dicts."""
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
