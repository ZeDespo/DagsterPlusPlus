"""
Core concepts for I/O bound services, such as how to organize files via an
ABC.
"""

import abc
import io
import json
import logging
from copy import deepcopy
from pathlib import Path
from typing import Self, TypeVar

import attrs

SOURCE = TypeVar("SOURCE")
"""Generic Type"""
FILE = TypeVar("FILE")
"""Generic Type"""
RESOURCE_CLIENT = TypeVar("RESOURCE_CLIENT")
"""Another generic type. If ingesting locally, this can be a NoneType"""


@attrs.define(eq=False)
class FileBufferPair[FILE, RESOURCE_CLIENT](abc.ABC):
    """
    Small dataclass to couple an object from some source with it's raw bytes
    representation. Wrapping the document data in this object allows us to
    fetch buffer data upon request, so we only use internet bandwith when necessary.
    """

    file: FILE = attrs.field()
    """A high-level representation of a file dictated by some other module."""
    buffer: io.BytesIO = attrs.field(repr=False, factory=io.BytesIO)
    """The bytes representation of the file as if we opened it in the file system"""
    metadata: dict = attrs.field(repr=False, factory=dict, kw_only=True)
    """Metadata that we know of ahead of time to attach to the document."""

    def __eq__(self, value: object, /) -> bool:
        """
        Equality is dependent on the filename only. We assume that there cannot
        be two files with the same name as a base case to cut down on
        complexity.
        """
        return isinstance(value, FileBufferPair) and self.filename == value.filename

    def __hash__(self) -> int:
        """Filename + metadata assigned to it."""
        return hash(f"{self.filename}{json.dumps(self.metadata)}")

    @abc.abstractmethod
    def download_contents(self, client: RESOURCE_CLIENT) -> None:
        """
        Download the file's content to the internal ``buffer`` attribute.
        Helps keep the lazy design pattern so we only download documents that we
        know we will need.
        """

    @property
    @abc.abstractmethod
    def filename(self) -> str:
        """Returns the filename for the pair, using the buffer as a reference."""
        return self.buffer.name

    def prepare_buffer_for_upload(self):
        """
        Prepare the buffer by setting the filename and set the file pointer to the
        beginning of the file.
        """
        self.buffer.name = self.filename
        self.buffer.seek(0)


FBP = TypeVar("FBP", bound=FileBufferPair)
"""GENERIC variable for file buffer pair objects"""


@attrs.define()
class DocumentIngressInterface[SOURCE, FILE, RESOURCE_CLIENT, FBP](abc.ABC):
    """
    ABC for structuring files togehter to prepare them for ingestion via some graph /
    op.

    Illustrates some I/O bound operation that retrieves files from some data source,
    and stores both the bytes and object representing that file.
    """

    source: SOURCE = attrs.field()
    """Where the files were found. Location's type is dependent on the data source."""
    files: list[FBP] = attrs.field(repr=False, factory=list)
    """Each item in the sequence represents one file found in the source."""
    _parent: FBP | None = attrs.field(default=None)
    """Points to the object that should be the parent."""
    _asset_key: str | list[str] | None = attrs.field(default=None)
    """If making an asset, this will be it's 'name'"""
    client: RESOURCE_CLIENT = attrs.field(default=None, converter=deepcopy, repr=False)
    """An API client to attach to the object to allow us to download files."""

    @property
    def asset_key(self) -> str | list[str] | None:
        """
        Return underlying asset key, so if it exists, we can declare this group
        of files as an external asset.
        """
        return self._asset_key

    def get_buffers(self) -> list[io.BytesIO]:
        """
        - Set the proper attributes to each buffer object.
        - Return the file's representation as a list of buffers.
        """
        buffers = []
        for f in self.files:
            f.prepare_buffer_for_upload()
            buffers.append(f.buffer)
        return buffers

    @property
    def files_as_original_objects(self) -> list[FILE]:
        """In case you need the original objects without a for loop all the time."""
        return [fb.file for fb in self.files]

    @classmethod
    @abc.abstractmethod
    def load_from_source(
        cls,
        source: SOURCE,
        client: RESOURCE_CLIENT,
        logger: logging.Logger,
        **kwargs,
    ) -> Self:
        """
        Grab the original objects from the provided source, download them, then
        instantiate the object with the underlying file representation buffers.

        :param source: Some object that represents where the files come from.
        :param client: The API resource to grab documents with.
        :param logger: A logger that keeps track of execution. Useful for keeping \
        track of downloaded files.
        :param args: Anything else to pass into the load function.
        :param kwargs: Same as args.
        :return: An instantiated class that is suited for ingress operations.
        """

    def remove_files(
        self,
        *,
        remove_directories: bool = True,
        remove_hidden_files: bool = True,
    ) -> Self:
        """
        Filter out the underlying ``files`` attribute if it meets certain criteria.
        This will NOT delete any object from the source, but just remove it from
        further processing in this object.

        :param remove_directories: Do not include directories in the underlying object.
        :param remove_hidden_files: Do not include hidden files.
        :return: The same object, to allow for method chaining.
        """
        supported_fbps = self.files
        if remove_hidden_files:
            supported_fbps = filter(
                lambda x: not x.filename.startswith("."), supported_fbps
            )
        if remove_directories:
            supported_fbps = filter(lambda x: not Path(x.file).is_dir(), supported_fbps)
        self.files = list(supported_fbps)
        return self
