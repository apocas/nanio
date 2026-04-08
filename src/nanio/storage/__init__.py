"""Storage backends for nanio."""

from nanio.storage.backend import (
    BucketInfo,
    ListResult,
    ObjectInfo,
    Storage,
)
from nanio.storage.filesystem import FilesystemStorage

__all__ = [
    "BucketInfo",
    "FilesystemStorage",
    "ListResult",
    "ObjectInfo",
    "Storage",
]
