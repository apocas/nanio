"""Decoder for the `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` body framing.

The aws-chunked encoding wraps the request body in length-prefixed frames,
each one signed with a SHA-256 HMAC of:

    "AWS4-HMAC-SHA256-PAYLOAD" \\n
    amz_date                    \\n
    credential_scope            \\n
    previous_signature          \\n
    sha256("")                  \\n
    sha256(chunk_data)

The first chunk's `previous_signature` is the request's seed signature
(the one that goes in the `Authorization` header). Each subsequent chunk
chains off the previous chunk's signature.

The decoder reads from a byte source (an `AsyncByteSource`) and yields
plain decoded bytes. The wire format is:

    <hex-size>;chunk-signature=<hex-sig>\\r\\n
    <chunk-bytes>
    \\r\\n
    ...
    0;chunk-signature=<hex-sig>\\r\\n
    \\r\\n

Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html
"""

from __future__ import annotations

import hashlib
import hmac
from collections.abc import AsyncIterator, Awaitable
from typing import Protocol

from nanio.auth.sigv4 import EMPTY_SHA256
from nanio.errors import InvalidRequest, SignatureDoesNotMatch

CHUNK_ALGORITHM = "AWS4-HMAC-SHA256-PAYLOAD"


class AsyncByteSource(Protocol):
    """An async source of bytes — receive a chunk or empty bytes at EOF."""

    def __call__(self) -> Awaitable[bytes]: ...


class _Buffered:
    """Single-buffer wrapper around an async byte source.

    Lets us read exact byte counts and lines without losing the tail of the
    underlying stream.
    """

    __slots__ = ("_buf", "_eof", "_source")

    def __init__(self, source: AsyncByteSource) -> None:
        self._source = source
        self._buf = bytearray()
        self._eof = False

    async def _pull(self) -> None:
        if self._eof:
            return
        chunk = await self._source()
        if not chunk:
            self._eof = True
            return
        self._buf.extend(chunk)

    async def read_exact(self, n: int) -> bytes:
        while len(self._buf) < n:
            if self._eof:
                raise InvalidRequest("unexpected EOF in aws-chunked body")
            await self._pull()
            if self._eof and len(self._buf) < n:
                raise InvalidRequest("unexpected EOF in aws-chunked body")
        out = bytes(self._buf[:n])
        del self._buf[:n]
        return out

    async def read_line(self, max_len: int = 4096) -> bytes:
        """Read until CRLF; return the line WITHOUT the CRLF."""
        while True:
            idx = self._buf.find(b"\r\n")
            if idx >= 0:
                line = bytes(self._buf[:idx])
                del self._buf[: idx + 2]
                return line
            if len(self._buf) > max_len:
                raise InvalidRequest("aws-chunked frame header too long")
            if self._eof:
                raise InvalidRequest("unexpected EOF reading aws-chunked frame header")
            await self._pull()


def _chunk_string_to_sign(
    *, amz_date: str, scope: str, previous_sig: str, chunk_sha256: str
) -> str:
    return "\n".join(
        [
            CHUNK_ALGORITHM,
            amz_date,
            scope,
            previous_sig,
            EMPTY_SHA256,
            chunk_sha256,
        ]
    )


def _sign_chunk(signing_key: bytes, string_to_sign: str) -> str:
    return hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()


async def decode_aws_chunked(
    source: AsyncByteSource,
    *,
    signing_key: bytes,
    seed_signature: str,
    amz_date: str,
    scope: str,
) -> AsyncIterator[bytes]:
    """Async-yield plain bytes from an aws-chunked encoded source.

    Verifies each chunk's signature on the way through. Raises
    `SignatureDoesNotMatch` if any chunk's signature is bad, or
    `InvalidRequest` if the framing is malformed.
    """
    buf = _Buffered(source)
    prev_sig = seed_signature

    while True:
        header = await buf.read_line()
        if not header:
            raise InvalidRequest("empty chunk header line")
        # Header is `<hex-size>;chunk-signature=<sig>` (and optionally other extensions).
        try:
            size_part, _, ext_part = header.decode("ascii").partition(";")
            size = int(size_part.strip(), 16)
        except (UnicodeDecodeError, ValueError) as exc:
            raise InvalidRequest(f"bad aws-chunked header: {header!r}") from exc

        chunk_sig: str | None = None
        for ext in ext_part.split(";"):
            if "=" not in ext:
                continue
            k, _, v = ext.partition("=")
            if k.strip() == "chunk-signature":
                chunk_sig = v.strip()
                break
        if chunk_sig is None:
            raise InvalidRequest("aws-chunked frame missing chunk-signature")

        if size > 0:
            data = await buf.read_exact(size)
        else:
            data = b""

        # Trailing CRLF after the chunk data.
        trailer = await buf.read_exact(2)
        if trailer != b"\r\n":
            raise InvalidRequest("missing CRLF terminator on aws-chunked frame")

        chunk_sha256 = hashlib.sha256(data).hexdigest()
        sts = _chunk_string_to_sign(
            amz_date=amz_date,
            scope=scope,
            previous_sig=prev_sig,
            chunk_sha256=chunk_sha256,
        )
        expected = _sign_chunk(signing_key, sts)
        if not hmac.compare_digest(expected, chunk_sig):
            raise SignatureDoesNotMatch()
        prev_sig = chunk_sig

        if size == 0:
            return
        yield data
