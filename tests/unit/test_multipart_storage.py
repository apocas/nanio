"""Tests for nanio.storage.multipart.MultipartManager."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import AsyncIterator

import pytest

from nanio.errors import (
    InvalidPart,
    InvalidPartOrder,
    NoSuchUpload,
)
from nanio.etag import unquote_etag
from nanio.storage.filesystem import FilesystemStorage
from nanio.storage.multipart import (
    MultipartInit,
    MultipartManager,
    new_upload_id,
)


@pytest.fixture
def storage(tmp_path):
    s = FilesystemStorage(tmp_path)
    s.create_bucket("widgets")
    return s


@pytest.fixture
def manager(tmp_path, storage):
    return MultipartManager(tmp_path)


async def _stream(data: bytes, chunk: int = 13) -> AsyncIterator[bytes]:
    for i in range(0, len(data), chunk):
        yield data[i : i + chunk]


def test_new_upload_id_unique():
    ids = {new_upload_id() for _ in range(50)}
    assert len(ids) == 50


def test_create_and_load_init(manager):
    init = MultipartInit(bucket="widgets", key="big.bin", content_type="application/octet-stream")
    upload_id = manager.create(init)
    loaded = manager.load_init(upload_id)
    assert loaded.bucket == "widgets"
    assert loaded.key == "big.bin"


def test_load_init_missing(manager):
    with pytest.raises(NoSuchUpload):
        manager.load_init("nonexistent")


def test_abort(manager):
    upload_id = manager.create(MultipartInit(bucket="widgets", key="k"))
    manager.abort(upload_id)
    with pytest.raises(NoSuchUpload):
        manager.load_init(upload_id)


def test_abort_missing(manager):
    with pytest.raises(NoSuchUpload):
        manager.abort("ghost")


def test_upload_part_round_trip(manager):
    upload_id = manager.create(MultipartInit(bucket="widgets", key="k"))
    info = asyncio.run(manager.upload_part(upload_id, 1, _stream(b"hello world")))
    assert info.size == 11
    expected_md5 = hashlib.md5(b"hello world", usedforsecurity=False).hexdigest()
    assert unquote_etag(info.etag) == expected_md5


def test_upload_part_missing_upload(manager):
    with pytest.raises(NoSuchUpload):
        asyncio.run(manager.upload_part("ghost", 1, _stream(b"x")))


def test_upload_part_invalid_part_number(manager):
    upload_id = manager.create(MultipartInit(bucket="widgets", key="k"))
    with pytest.raises(InvalidPart):
        asyncio.run(manager.upload_part(upload_id, 0, _stream(b"x")))
    with pytest.raises(InvalidPart):
        asyncio.run(manager.upload_part(upload_id, 10001, _stream(b"x")))


def test_list_parts_sorted(manager):
    upload_id = manager.create(MultipartInit(bucket="widgets", key="k"))
    asyncio.run(manager.upload_part(upload_id, 3, _stream(b"three")))
    asyncio.run(manager.upload_part(upload_id, 1, _stream(b"one")))
    asyncio.run(manager.upload_part(upload_id, 2, _stream(b"two")))
    parts = manager.list_parts(upload_id)
    assert [p.part_number for p in parts] == [1, 2, 3]


def test_list_parts_missing_upload(manager):
    with pytest.raises(NoSuchUpload):
        manager.list_parts("ghost")


def test_complete_round_trip(manager, storage, tmp_path):
    upload_id = manager.create(
        MultipartInit(bucket="widgets", key="big.bin", content_type="application/octet-stream")
    )
    p1 = asyncio.run(manager.upload_part(upload_id, 1, _stream(b"hello ")))
    p2 = asyncio.run(manager.upload_part(upload_id, 2, _stream(b"world")))
    info = manager.complete(
        upload_id,
        [(1, p1.etag), (2, p2.etag)],
    )
    assert info.size == 11
    # Multipart ETag must end in -2.
    assert info.etag.endswith('-2"')
    # The object is now visible via the storage layer.
    head = storage.head_object("widgets", "big.bin")
    assert head.size == 11


def test_complete_with_missing_part(manager):
    upload_id = manager.create(MultipartInit(bucket="widgets", key="k"))
    p1 = asyncio.run(manager.upload_part(upload_id, 1, _stream(b"x")))
    with pytest.raises(InvalidPart):
        manager.complete(upload_id, [(1, p1.etag), (2, '"deadbeef"')])


def test_complete_with_wrong_etag(manager):
    upload_id = manager.create(MultipartInit(bucket="widgets", key="k"))
    asyncio.run(manager.upload_part(upload_id, 1, _stream(b"x")))
    with pytest.raises(InvalidPart):
        manager.complete(upload_id, [(1, '"wrongetag"')])


def test_complete_unordered_parts(manager):
    upload_id = manager.create(MultipartInit(bucket="widgets", key="k"))
    p1 = asyncio.run(manager.upload_part(upload_id, 1, _stream(b"a")))
    p2 = asyncio.run(manager.upload_part(upload_id, 2, _stream(b"b")))
    with pytest.raises(InvalidPartOrder):
        manager.complete(upload_id, [(2, p2.etag), (1, p1.etag)])


def test_complete_no_parts(manager):
    upload_id = manager.create(MultipartInit(bucket="widgets", key="k"))
    with pytest.raises(InvalidPart):
        manager.complete(upload_id, [])


def test_complete_overwrites_existing_object(manager, storage):
    """Multipart Complete must overwrite a pre-existing object atomically."""
    asyncio.run(storage.put_object("widgets", "big.bin", _stream(b"old data")))
    upload_id = manager.create(MultipartInit(bucket="widgets", key="big.bin"))
    p1 = asyncio.run(manager.upload_part(upload_id, 1, _stream(b"new data here")))
    info = manager.complete(upload_id, [(1, p1.etag)])
    assert info.size == 13
    head = storage.head_object("widgets", "big.bin")
    assert head.size == 13


def test_list_uploads(manager):
    u1 = manager.create(MultipartInit(bucket="widgets", key="k1"))
    u2 = manager.create(MultipartInit(bucket="widgets", key="k2"))
    listed = manager.list_uploads()
    ids = {upload_id for upload_id, _ in listed}
    assert u1 in ids and u2 in ids


def test_warn_about_abandoned_uploads(manager):
    """Anything older than max_age should be reported."""
    upload_id = manager.create(MultipartInit(bucket="widgets", key="k"))
    # Force an ancient initiated time by rewriting init.json
    init = manager.load_init(upload_id)
    from datetime import datetime, timedelta, timezone

    init.initiated = datetime.now(tz=timezone.utc) - timedelta(days=30)
    from nanio.storage.multipart import _init_to_dict
    import json

    from nanio.storage.paths import multipart_init_path

    p = multipart_init_path(manager._data_dir, upload_id)
    with open(p, "w") as f:
        json.dump(_init_to_dict(init), f)

    abandoned = manager.warn_about_abandoned_uploads(max_age_seconds=7 * 24 * 3600)
    assert any(uid == upload_id for uid, _ in abandoned)


def test_concurrent_part_uploads(manager):
    """50 parts uploaded in parallel must all be visible after."""
    upload_id = manager.create(MultipartInit(bucket="widgets", key="k"))

    async def go():
        await asyncio.gather(
            *[
                manager.upload_part(upload_id, n, _stream(f"chunk-{n:03d}".encode()))
                for n in range(1, 51)
            ]
        )

    asyncio.run(go())
    parts = manager.list_parts(upload_id)
    assert [p.part_number for p in parts] == list(range(1, 51))
