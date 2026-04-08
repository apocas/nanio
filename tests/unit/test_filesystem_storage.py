"""Tests for nanio.storage.filesystem.FilesystemStorage."""

from __future__ import annotations

import asyncio
import base64
import hashlib
from collections.abc import AsyncIterator

import pytest

from nanio.errors import (
    BadDigest,
    BucketAlreadyOwnedByYou,
    BucketNotEmpty,
    InvalidArgument,
    NoSuchBucket,
    NoSuchKey,
)
from nanio.storage.filesystem import FilesystemStorage


@pytest.fixture
def storage(tmp_path):
    return FilesystemStorage(tmp_path)


async def _stream(data: bytes, chunk: int = 13) -> AsyncIterator[bytes]:
    for i in range(0, len(data), chunk):
        yield data[i : i + chunk]


# ----------------------------------------------------------------------
# Bucket ops
# ----------------------------------------------------------------------


def test_create_and_head_bucket(storage):
    info = storage.create_bucket("widgets")
    assert info.name == "widgets"
    head = storage.head_bucket("widgets")
    assert head.name == "widgets"


def test_create_bucket_twice_raises(storage):
    storage.create_bucket("widgets")
    with pytest.raises(BucketAlreadyOwnedByYou):
        storage.create_bucket("widgets")


def test_head_bucket_missing(storage):
    with pytest.raises(NoSuchBucket):
        storage.head_bucket("widgets")


def test_list_buckets_sorted(storage):
    storage.create_bucket("zebra")
    storage.create_bucket("alpha")
    storage.create_bucket("middle")
    names = [b.name for b in storage.list_buckets()]
    assert names == ["alpha", "middle", "zebra"]


def test_delete_empty_bucket(storage):
    storage.create_bucket("widgets")
    storage.delete_bucket("widgets")
    with pytest.raises(NoSuchBucket):
        storage.head_bucket("widgets")


def test_delete_non_empty_bucket(storage):
    storage.create_bucket("widgets")
    asyncio.run(storage.put_object("widgets", "foo.txt", _stream(b"hi")))
    with pytest.raises(BucketNotEmpty):
        storage.delete_bucket("widgets")


def test_delete_missing_bucket(storage):
    with pytest.raises(NoSuchBucket):
        storage.delete_bucket("widgets")


# ----------------------------------------------------------------------
# Put / head / get / delete object
# ----------------------------------------------------------------------


def test_put_then_head(storage):
    storage.create_bucket("widgets")
    info = asyncio.run(
        storage.put_object("widgets", "hello.txt", _stream(b"hello world"), content_type="text/plain")
    )
    assert info.size == 11
    assert info.etag == f'"{hashlib.md5(b"hello world", usedforsecurity=False).hexdigest()}"'
    assert info.content_type == "text/plain"

    head = storage.head_object("widgets", "hello.txt")
    assert head.size == 11
    assert head.etag == info.etag


def test_put_to_missing_bucket(storage):
    with pytest.raises(NoSuchBucket):
        asyncio.run(storage.put_object("ghost", "k", _stream(b"x")))


def test_put_with_subkey_creates_dirs(storage):
    storage.create_bucket("widgets")
    asyncio.run(storage.put_object("widgets", "a/b/c.txt", _stream(b"deep")))
    assert storage.head_object("widgets", "a/b/c.txt").size == 4


def test_put_with_correct_md5(storage):
    storage.create_bucket("widgets")
    payload = b"hello"
    md5 = base64.b64encode(hashlib.md5(payload, usedforsecurity=False).digest()).decode()
    info = asyncio.run(
        storage.put_object("widgets", "k", _stream(payload), expected_md5=md5)
    )
    assert info.size == 5


def test_put_with_bad_md5(storage):
    storage.create_bucket("widgets")
    bad = base64.b64encode(b"\x00" * 16).decode()
    with pytest.raises(BadDigest):
        asyncio.run(
            storage.put_object("widgets", "k", _stream(b"hello"), expected_md5=bad)
        )
    # The temp file must NOT be left behind, and the object must NOT exist.
    with pytest.raises(NoSuchKey):
        storage.head_object("widgets", "k")


def test_get_object_streams_full_body(storage):
    storage.create_bucket("widgets")
    payload = b"the quick brown fox" * 100
    asyncio.run(storage.put_object("widgets", "k", _stream(payload)))

    async def _read():
        result = await storage.get_object("widgets", "k")
        out = b""
        async for chunk in result.body:
            out += chunk
        return out, result.info

    body, info = asyncio.run(_read())
    assert body == payload
    assert info.size == len(payload)


def test_get_object_range(storage):
    storage.create_bucket("widgets")
    payload = b"abcdefghijklmnopqrstuvwxyz"
    asyncio.run(storage.put_object("widgets", "k", _stream(payload)))

    async def _read(start, end):
        result = await storage.get_object("widgets", "k", range_start=start, range_end=end)
        out = b""
        async for chunk in result.body:
            out += chunk
        return out, result

    body, result = asyncio.run(_read(0, 4))
    assert body == b"abcde"
    assert result.range_start == 0
    assert result.range_end == 4

    body, _ = asyncio.run(_read(10, 14))
    assert body == b"klmno"

    body, _ = asyncio.run(_read(20, 25))
    assert body == b"uvwxyz"


def test_get_invalid_range(storage):
    storage.create_bucket("widgets")
    asyncio.run(storage.put_object("widgets", "k", _stream(b"hello")))
    with pytest.raises(InvalidArgument):
        asyncio.run(storage.get_object("widgets", "k", range_start=10, range_end=20))


def test_get_missing_key(storage):
    storage.create_bucket("widgets")
    with pytest.raises(NoSuchKey):
        asyncio.run(storage.get_object("widgets", "missing"))


def test_head_missing_key(storage):
    storage.create_bucket("widgets")
    with pytest.raises(NoSuchKey):
        storage.head_object("widgets", "missing")


def test_delete_object(storage):
    storage.create_bucket("widgets")
    asyncio.run(storage.put_object("widgets", "a/b/c.txt", _stream(b"x")))
    storage.delete_object("widgets", "a/b/c.txt")
    with pytest.raises(NoSuchKey):
        storage.head_object("widgets", "a/b/c.txt")


def test_delete_object_idempotent(storage):
    storage.create_bucket("widgets")
    storage.delete_object("widgets", "never-existed")  # no error


def test_delete_prunes_empty_subdirs(storage, tmp_path):
    storage.create_bucket("widgets")
    asyncio.run(storage.put_object("widgets", "deep/path/file.txt", _stream(b"x")))
    storage.delete_object("widgets", "deep/path/file.txt")
    # The deep/ subtree should be gone.
    assert not (tmp_path / "widgets" / "deep").exists()
    # But the bucket itself remains.
    assert (tmp_path / "widgets").is_dir()


# ----------------------------------------------------------------------
# Listing
# ----------------------------------------------------------------------


def _put(storage, bucket, key, body=b"x"):
    asyncio.run(storage.put_object(bucket, key, _stream(body)))


def test_list_objects_basic(storage):
    storage.create_bucket("widgets")
    _put(storage, "widgets", "alpha.txt")
    _put(storage, "widgets", "beta.txt")
    _put(storage, "widgets", "gamma.txt")
    result = storage.list_objects("widgets")
    keys = [c.key for c in result.contents]
    assert keys == ["alpha.txt", "beta.txt", "gamma.txt"]
    assert result.is_truncated is False
    assert result.common_prefixes == []


def test_list_objects_with_prefix(storage):
    storage.create_bucket("widgets")
    _put(storage, "widgets", "logs/2026/jan.txt")
    _put(storage, "widgets", "logs/2026/feb.txt")
    _put(storage, "widgets", "data/raw.bin")
    result = storage.list_objects("widgets", prefix="logs/")
    keys = [c.key for c in result.contents]
    assert sorted(keys) == ["logs/2026/feb.txt", "logs/2026/jan.txt"]


def test_list_objects_with_delimiter(storage):
    storage.create_bucket("widgets")
    _put(storage, "widgets", "logs/jan.txt")
    _put(storage, "widgets", "logs/feb.txt")
    _put(storage, "widgets", "data/raw.bin")
    _put(storage, "widgets", "top.txt")
    result = storage.list_objects("widgets", delimiter="/")
    keys = [c.key for c in result.contents]
    assert keys == ["top.txt"]
    assert sorted(result.common_prefixes) == ["data/", "logs/"]


def test_list_objects_max_keys_truncation(storage):
    storage.create_bucket("widgets")
    for i in range(10):
        _put(storage, "widgets", f"k-{i:02d}.txt")
    result = storage.list_objects("widgets", max_keys=3)
    assert len(result.contents) == 3
    assert result.is_truncated is True
    assert result.next_continuation_token is not None
    assert [c.key for c in result.contents] == ["k-00.txt", "k-01.txt", "k-02.txt"]


def test_list_objects_pagination_round_trip(storage):
    storage.create_bucket("widgets")
    for i in range(7):
        _put(storage, "widgets", f"k-{i}.txt")
    page1 = storage.list_objects("widgets", max_keys=3)
    page2 = storage.list_objects("widgets", max_keys=3, continuation_token=page1.next_continuation_token)
    page3 = storage.list_objects("widgets", max_keys=3, continuation_token=page2.next_continuation_token)
    keys = [c.key for c in page1.contents + page2.contents + page3.contents]
    assert keys == [f"k-{i}.txt" for i in range(7)]
    assert page3.is_truncated is False


def test_list_objects_max_keys_zero(storage):
    storage.create_bucket("widgets")
    _put(storage, "widgets", "a.txt")
    result = storage.list_objects("widgets", max_keys=0)
    assert result.contents == []
    assert result.is_truncated is False


def test_list_objects_missing_bucket(storage):
    with pytest.raises(NoSuchBucket):
        storage.list_objects("ghost")


def test_list_objects_skips_metadata_dir(storage, tmp_path):
    storage.create_bucket("widgets")
    _put(storage, "widgets", "a.txt")
    # The metadata sidecar exists under .nanio-meta/ — make sure it doesn't show up
    result = storage.list_objects("widgets")
    keys = [c.key for c in result.contents]
    assert keys == ["a.txt"]


# ----------------------------------------------------------------------
# Copy
# ----------------------------------------------------------------------


def test_copy_object(storage):
    storage.create_bucket("src")
    storage.create_bucket("dst")
    asyncio.run(
        storage.put_object("src", "k.txt", _stream(b"hello"), content_type="text/plain")
    )
    info = asyncio.run(storage.copy_object("src", "k.txt", "dst", "k.txt"))
    assert info.size == 5
    assert info.content_type == "text/plain"
    assert storage.head_object("dst", "k.txt").size == 5
