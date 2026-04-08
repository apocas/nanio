"""Unit tests for the aws-chunked / STREAMING-AWS4-HMAC-SHA256-PAYLOAD decoder.

We construct chunked frames manually using our own signing primitives —
which test_sigv4 has already proven match boto3 bit-for-bit on the seed
signature path. From there, the chunk-signature math is well-defined by
the AWS spec, and the decoder must accept exactly the same chains it
would produce.
"""

from __future__ import annotations

import hashlib
import hmac

import pytest

from nanio.auth.chunked import (
    CHUNK_ALGORITHM,
    decode_aws_chunked,
)
from nanio.auth.sigv4 import EMPTY_SHA256, derive_signing_key
from nanio.errors import InvalidRequest, SignatureDoesNotMatch

SECRET_KEY = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
DATE = "20240101"
REGION = "us-east-1"
SERVICE = "s3"
SCOPE = f"{DATE}/{REGION}/{SERVICE}/aws4_request"
AMZ_DATE = "20240101T120000Z"
SEED_SIG = "0" * 64

SIGNING_KEY = derive_signing_key(SECRET_KEY, date=DATE, region=REGION, service=SERVICE)


def _sign_chunk(prev_sig: str, chunk: bytes) -> str:
    sts = "\n".join(
        [
            CHUNK_ALGORITHM,
            AMZ_DATE,
            SCOPE,
            prev_sig,
            EMPTY_SHA256,
            hashlib.sha256(chunk).hexdigest(),
        ]
    )
    return hmac.new(SIGNING_KEY, sts.encode("utf-8"), hashlib.sha256).hexdigest()


def _build_chunked_body(chunks: list[bytes]) -> bytes:
    out = bytearray()
    prev = SEED_SIG
    for chunk in chunks:
        sig = _sign_chunk(prev, chunk)
        out.extend(f"{len(chunk):x};chunk-signature={sig}\r\n".encode("ascii"))
        out.extend(chunk)
        out.extend(b"\r\n")
        prev = sig
    # Final empty chunk
    final_sig = _sign_chunk(prev, b"")
    out.extend(f"0;chunk-signature={final_sig}\r\n\r\n".encode("ascii"))
    return bytes(out)


def _make_source(data: bytes, chunk_size: int = 17):
    """Build a callable that yields `data` in fixed-size chunks (then EOF)."""
    pos = 0

    async def _source() -> bytes:
        nonlocal pos
        if pos >= len(data):
            return b""
        end = min(len(data), pos + chunk_size)
        out = data[pos:end]
        pos = end
        return out

    return _source


async def _drain(decoder):
    out = bytearray()
    async for chunk in decoder:
        out.extend(chunk)
    return bytes(out)


@pytest.mark.asyncio
async def test_single_chunk():
    payload = b"hello world"
    body = _build_chunked_body([payload])
    decoder = decode_aws_chunked(
        _make_source(body),
        signing_key=SIGNING_KEY,
        seed_signature=SEED_SIG,
        amz_date=AMZ_DATE,
        scope=SCOPE,
    )
    assert await _drain(decoder) == payload


@pytest.mark.asyncio
async def test_multi_chunk():
    chunks = [b"first ", b"second ", b"third"]
    body = _build_chunked_body(chunks)
    decoder = decode_aws_chunked(
        _make_source(body, chunk_size=8),
        signing_key=SIGNING_KEY,
        seed_signature=SEED_SIG,
        amz_date=AMZ_DATE,
        scope=SCOPE,
    )
    assert await _drain(decoder) == b"first second third"


@pytest.mark.asyncio
async def test_one_byte_chunks():
    """Worst case: data arrives one byte at a time from the source."""
    chunks = [b"abc", b"def", b"ghi"]
    body = _build_chunked_body(chunks)
    decoder = decode_aws_chunked(
        _make_source(body, chunk_size=1),
        signing_key=SIGNING_KEY,
        seed_signature=SEED_SIG,
        amz_date=AMZ_DATE,
        scope=SCOPE,
    )
    assert await _drain(decoder) == b"abcdefghi"


@pytest.mark.asyncio
async def test_large_chunk():
    payload = b"x" * 65536
    body = _build_chunked_body([payload])
    decoder = decode_aws_chunked(
        _make_source(body, chunk_size=4096),
        signing_key=SIGNING_KEY,
        seed_signature=SEED_SIG,
        amz_date=AMZ_DATE,
        scope=SCOPE,
    )
    assert await _drain(decoder) == payload


@pytest.mark.asyncio
async def test_bad_chunk_signature():
    payload = b"hello"
    body = _build_chunked_body([payload])
    # Corrupt the signature: replace the first hex char of the chunk sig.
    pos = body.index(b"chunk-signature=") + len("chunk-signature=")
    bad = bytearray(body)
    bad[pos] = ord("0") if bad[pos] != ord("0") else ord("1")
    decoder = decode_aws_chunked(
        _make_source(bytes(bad)),
        signing_key=SIGNING_KEY,
        seed_signature=SEED_SIG,
        amz_date=AMZ_DATE,
        scope=SCOPE,
    )
    with pytest.raises(SignatureDoesNotMatch):
        await _drain(decoder)


@pytest.mark.asyncio
async def test_truncated_body():
    payload = b"hello"
    body = _build_chunked_body([payload])[:30]  # cut mid-frame
    decoder = decode_aws_chunked(
        _make_source(body),
        signing_key=SIGNING_KEY,
        seed_signature=SEED_SIG,
        amz_date=AMZ_DATE,
        scope=SCOPE,
    )
    with pytest.raises((InvalidRequest, SignatureDoesNotMatch)):
        await _drain(decoder)


@pytest.mark.asyncio
async def test_missing_chunk_signature_extension():
    body = b"5\r\nhello\r\n0\r\n\r\n"
    decoder = decode_aws_chunked(
        _make_source(body),
        signing_key=SIGNING_KEY,
        seed_signature=SEED_SIG,
        amz_date=AMZ_DATE,
        scope=SCOPE,
    )
    with pytest.raises(InvalidRequest):
        await _drain(decoder)


@pytest.mark.asyncio
async def test_zero_chunks_just_terminator():
    body = _build_chunked_body([])
    decoder = decode_aws_chunked(
        _make_source(body),
        signing_key=SIGNING_KEY,
        seed_signature=SEED_SIG,
        amz_date=AMZ_DATE,
        scope=SCOPE,
    )
    assert await _drain(decoder) == b""
