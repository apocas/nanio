"""Abstract `Storage` protocol that handlers talk to.

Defining this as a Protocol (rather than a single concrete class) means
the handlers are decoupled from the filesystem and we can stub the storage
layer in tests, or add alternate backends later (in-memory for tests,
S3-on-S3 for proxying, etc.).

Every method that returns object bytes returns an *async iterator* of
`bytes` chunks, never a fully-buffered `bytes` value. This is the single
most important design rule of the storage layer — it preserves the
zero-buffering streaming property end-to-end.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol


@dataclass(slots=True)
class BucketInfo:
    name: str
    created: datetime


@dataclass(slots=True)
class ObjectInfo:
    """Metadata for a single object — what HEAD/GET/LIST need to know."""

    key: str
    size: int
    etag: str  # quoted, e.g. '"d41..."'
    last_modified: datetime
    content_type: str = "application/octet-stream"
    user_metadata: Mapping[str, str] = field(default_factory=dict)
    storage_class: str = "STANDARD"
    content_encoding: str | None = None
    content_disposition: str | None = None
    cache_control: str | None = None


@dataclass(slots=True)
class ListResult:
    contents: list[ObjectInfo]
    common_prefixes: list[str]
    is_truncated: bool
    next_continuation_token: str | None


@dataclass(slots=True)
class GetObjectResult:
    info: ObjectInfo
    body: AsyncIterator[bytes]
    # If the response is a Range request, these reflect the served slice.
    range_start: int | None = None
    range_end: int | None = None  # inclusive


class Storage(Protocol):
    """Storage backend protocol that all nanio handlers depend on."""

    # ---- bucket ops ------------------------------------------------------

    def create_bucket(self, bucket: str) -> BucketInfo: ...

    def delete_bucket(self, bucket: str) -> None: ...

    def head_bucket(self, bucket: str) -> BucketInfo: ...

    def list_buckets(self) -> list[BucketInfo]: ...

    # ---- object ops ------------------------------------------------------

    async def put_object(
        self,
        bucket: str,
        key: str,
        body: AsyncIterator[bytes],
        *,
        content_type: str = "application/octet-stream",
        user_metadata: Mapping[str, str] | None = None,
        content_encoding: str | None = None,
        content_disposition: str | None = None,
        cache_control: str | None = None,
        expected_md5: str | None = None,
    ) -> ObjectInfo: ...

    async def get_object(
        self,
        bucket: str,
        key: str,
        *,
        range_start: int | None = None,
        range_end: int | None = None,
    ) -> GetObjectResult: ...

    def head_object(self, bucket: str, key: str) -> ObjectInfo: ...

    def delete_object(self, bucket: str, key: str) -> None: ...

    def list_objects(
        self,
        bucket: str,
        *,
        prefix: str = "",
        delimiter: str | None = None,
        max_keys: int = 1000,
        continuation_token: str | None = None,
        start_after: str | None = None,
    ) -> ListResult: ...

    async def copy_object(
        self,
        src_bucket: str,
        src_key: str,
        dst_bucket: str,
        dst_key: str,
    ) -> ObjectInfo: ...
