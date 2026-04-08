"""ETag computation.

S3 ETags follow two formats:

1. Simple PUT: the literal hex MD5 of the object body, wrapped in quotes.
   Example: ``"d41d8cd98f00b204e9800998ecf8427e"``

2. Multipart upload: the hex MD5 of the *concatenation of the binary MD5s of
   each part*, followed by ``-N`` where N is the number of parts. Wrapped in
   quotes.
   Example: ``"e2c1f1e7e2f5c2c0e6b6a8e6c5b9d8f9-3"``

The "quotes are part of the value" thing is real — every S3 client expects
the ``ETag`` header to literally include the surrounding double quotes.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable


def quote_etag(hex_digest: str) -> str:
    """Wrap a bare hex digest in S3-style quotes."""
    return f'"{hex_digest}"'


def unquote_etag(etag: str) -> str:
    """Strip S3-style surrounding quotes if present."""
    if len(etag) >= 2 and etag.startswith('"') and etag.endswith('"'):
        return etag[1:-1]
    return etag


def md5_hex(data: bytes) -> str:
    return hashlib.md5(data, usedforsecurity=False).hexdigest()


def simple_etag(data: bytes) -> str:
    """Compute the quoted ETag for a single-PUT object body."""
    return quote_etag(md5_hex(data))


def multipart_etag(part_md5s_hex: Iterable[str]) -> str:
    """Compute the quoted multipart ETag from per-part hex MD5s.

    Per the S3 spec: ``md5(concat(bytes.fromhex(part_md5) for part in parts)) + "-N"``.
    """
    digest = hashlib.md5(usedforsecurity=False)
    count = 0
    for part_hex in part_md5s_hex:
        digest.update(bytes.fromhex(part_hex))
        count += 1
    if count == 0:
        raise ValueError("multipart_etag requires at least one part")
    return quote_etag(f"{digest.hexdigest()}-{count}")


class StreamingMd5:
    """Incremental MD5 hasher for use while streaming a body to disk."""

    __slots__ = ("_h", "_size")

    def __init__(self) -> None:
        self._h = hashlib.md5(usedforsecurity=False)
        self._size = 0

    def update(self, chunk: bytes) -> None:
        self._h.update(chunk)
        self._size += len(chunk)

    @property
    def size(self) -> int:
        return self._size

    def hexdigest(self) -> str:
        return self._h.hexdigest()

    def quoted_etag(self) -> str:
        return quote_etag(self.hexdigest())
