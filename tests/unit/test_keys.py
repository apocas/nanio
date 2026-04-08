"""Tests for nanio.keys validation and safe_join."""

from __future__ import annotations

from pathlib import Path

import pytest

from nanio.errors import InvalidBucketName, InvalidObjectName
from nanio.keys import safe_join, validate_bucket_name, validate_object_key


# ---- bucket names ----------------------------------------------------------


@pytest.mark.parametrize(
    "name",
    [
        "abc",
        "my-bucket",
        "my.bucket",
        "a1b2c3",
        "0abc",
        "abcdefghij" * 6 + "abc",  # 63 chars
    ],
)
def test_valid_bucket_names(name):
    validate_bucket_name(name)  # should not raise


@pytest.mark.parametrize(
    "name",
    [
        "ab",  # too short
        "a" * 64,  # too long
        "ABC",  # uppercase
        "-abc",  # leading hyphen
        "abc-",  # trailing hyphen
        "a..b",  # consecutive dots
        "192.168.1.1",  # ip-formatted
        ".nanio-secret",  # reserved prefix
        "",
    ],
)
def test_invalid_bucket_names(name):
    with pytest.raises(InvalidBucketName):
        validate_bucket_name(name)


# ---- object keys -----------------------------------------------------------


@pytest.mark.parametrize(
    "key",
    [
        "a",
        "foo.txt",
        "path/to/file.bin",
        "deeply/nested/path/with/many/segments.txt",
        "spaces are fine.txt",
        "unicode-🦀.txt",
        "a" * 1024,
    ],
)
def test_valid_object_keys(key):
    validate_object_key(key)


@pytest.mark.parametrize(
    "key",
    [
        "",
        "/leading-slash",
        "trailing/",
        "//double",
        "a/./b",
        "a/../b",
        "..",
        ".",
        ".nanio-meta/foo",
        "with\x00null",
        "with\nnewline",
        "a" * 1025,  # > 1024 bytes
    ],
)
def test_invalid_object_keys(key):
    with pytest.raises(InvalidObjectName):
        validate_object_key(key)


def test_object_key_max_bytes_in_utf8():
    # 4-byte chars push us over the byte limit even when char count is small.
    too_long = "🦀" * 257  # 257 * 4 = 1028 bytes
    with pytest.raises(InvalidObjectName):
        validate_object_key(too_long)


# ---- safe_join -------------------------------------------------------------


def test_safe_join_basic(tmp_path):
    base = tmp_path.resolve()
    p = safe_join(base, "bucket", "key.txt")
    assert p == base / "bucket" / "key.txt"


def test_safe_join_with_subpath(tmp_path):
    base = tmp_path.resolve()
    p = safe_join(base, "bucket", "a/b/c.txt")
    assert p == base / "bucket" / "a" / "b" / "c.txt"


def test_safe_join_rejects_dotdot(tmp_path):
    base = tmp_path.resolve()
    with pytest.raises(ValueError):
        safe_join(base, "bucket", "../escape.txt")


def test_safe_join_rejects_absolute_part(tmp_path):
    base = tmp_path.resolve()
    with pytest.raises(ValueError):
        safe_join(base, "/etc/passwd")


def test_safe_join_requires_absolute_base():
    with pytest.raises(ValueError):
        safe_join(Path("relative/base"), "x")
