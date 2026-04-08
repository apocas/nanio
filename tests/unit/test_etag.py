"""Tests for nanio.etag MD5 + multipart ETag computation."""

from __future__ import annotations

import hashlib

import pytest

from nanio.etag import (
    StreamingMd5,
    md5_hex,
    multipart_etag,
    quote_etag,
    simple_etag,
    unquote_etag,
)


def test_quote_unquote_round_trip():
    assert quote_etag("abc") == '"abc"'
    assert unquote_etag('"abc"') == "abc"
    assert unquote_etag("abc") == "abc"  # already unquoted


def test_md5_hex_known_value():
    # MD5 of empty bytes — sanity-check the well-known constant.
    assert md5_hex(b"") == "d41d8cd98f00b204e9800998ecf8427e"


def test_simple_etag_includes_quotes():
    e = simple_etag(b"hello")
    assert e.startswith('"') and e.endswith('"')
    assert e == f'"{hashlib.md5(b"hello", usedforsecurity=False).hexdigest()}"'


def test_multipart_etag_format():
    # Use 3 trivial part hashes; the result must end in `-3`.
    parts = [
        "00000000000000000000000000000000",
        "11111111111111111111111111111111",
        "22222222222222222222222222222222",
    ]
    etag = multipart_etag(parts)
    assert etag.startswith('"')
    assert etag.endswith('-3"')
    inner = unquote_etag(etag)
    digest_hex, count = inner.split("-")
    assert count == "3"
    expected = hashlib.md5(
        bytes.fromhex(parts[0]) + bytes.fromhex(parts[1]) + bytes.fromhex(parts[2]),
        usedforsecurity=False,
    ).hexdigest()
    assert digest_hex == expected


def test_multipart_etag_single_part_still_uses_format():
    etag = multipart_etag(["00000000000000000000000000000000"])
    assert etag.endswith('-1"')


def test_multipart_etag_zero_parts_raises():
    with pytest.raises(ValueError):
        multipart_etag([])


def test_streaming_md5_matches_oneshot():
    payload = b"the quick brown fox jumps over the lazy dog"
    s = StreamingMd5()
    s.update(payload[:10])
    s.update(payload[10:])
    assert s.size == len(payload)
    assert s.hexdigest() == hashlib.md5(payload, usedforsecurity=False).hexdigest()
    assert s.quoted_etag() == f'"{s.hexdigest()}"'
