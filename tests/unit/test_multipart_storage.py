"""Tests for nanio.storage.multipart.MultipartManager."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

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
    _init_to_dict,
    new_upload_id,
)
from nanio.storage.paths import multipart_dir, multipart_init_path


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


def _make_ancient_upload(manager, key: str = "k", days_old: int = 30) -> str:
    """Create a multipart upload then back-date its init.json AND its
    directory mtime so the GC recency gate doesn't skip it."""
    upload_id = manager.create(MultipartInit(bucket="widgets", key=key))
    init = manager.load_init(upload_id)
    init.initiated = datetime.now(tz=UTC) - timedelta(days=days_old)

    p = multipart_init_path(manager._data_dir, upload_id)
    with open(p, "w") as f:
        json.dump(_init_to_dict(init), f)

    d = multipart_dir(manager._data_dir, upload_id)
    ancient_ts = (datetime.now(tz=UTC) - timedelta(days=days_old)).timestamp()
    os.utime(d, (ancient_ts, ancient_ts))
    return upload_id


def test_warn_about_abandoned_uploads(manager):
    """Anything older than max_age should be reported."""
    upload_id = _make_ancient_upload(manager)
    abandoned = manager.warn_about_abandoned_uploads(max_age_seconds=7 * 24 * 3600)
    assert any(uid == upload_id for uid, _ in abandoned)


def test_gc_abandoned_uploads_deletes_old(manager):
    """Security audit M4: gc_abandoned_uploads must actually delete old
    upload dirs and return their IDs."""
    old_id = _make_ancient_upload(manager, key="old")
    young_id = manager.create(MultipartInit(bucket="widgets", key="young"))

    deleted = manager.gc_abandoned_uploads(max_age_seconds=7 * 24 * 3600)
    assert old_id in deleted
    assert young_id not in deleted

    # Old upload's dir is gone; young one is still there.
    with pytest.raises(NoSuchUpload):
        manager.load_init(old_id)
    manager.load_init(young_id)  # must not raise


def test_gc_abandoned_uploads_empty_when_none_old(manager):
    manager.create(MultipartInit(bucket="widgets", key="k"))
    deleted = manager.gc_abandoned_uploads(max_age_seconds=7 * 24 * 3600)
    assert deleted == []


def test_list_uploads_skips_corrupt_init_json(manager):
    """A single corrupt init.json must not crash list_uploads — before
    the fix, a propagated JSONDecodeError took down ListMultipartUploads,
    the GC, and the startup warning sweep."""
    good_id = manager.create(MultipartInit(bucket="widgets", key="good"))
    bad_id = manager.create(MultipartInit(bucket="widgets", key="bad"))
    multipart_init_path(manager._data_dir, bad_id).write_text("{ this is not json ]")

    ids = {uid for uid, _ in manager.list_uploads()}
    assert good_id in ids
    assert bad_id not in ids


def test_list_uploads_skips_init_json_missing_required_fields(manager):
    good_id = manager.create(MultipartInit(bucket="widgets", key="good"))
    bad_id = manager.create(MultipartInit(bucket="widgets", key="bad"))
    multipart_init_path(manager._data_dir, bad_id).write_text('{"unexpected": "field"}')

    ids = {uid for uid, _ in manager.list_uploads()}
    assert good_id in ids
    assert bad_id not in ids


def test_gc_orphan_sweep_deletes_corrupt_upload_dirs(manager):
    """The GC's orphan path must delete upload dirs whose init.json is
    corrupt and whose dir mtime is past the cutoff, so corrupt state
    can never accumulate indefinitely."""
    bad_id = manager.create(MultipartInit(bucket="widgets", key="bad"))
    multipart_init_path(manager._data_dir, bad_id).write_text("{ corrupt")

    d = multipart_dir(manager._data_dir, bad_id)
    ancient_ts = (datetime.now(tz=UTC) - timedelta(days=30)).timestamp()
    os.utime(d, (ancient_ts, ancient_ts))

    deleted = manager.gc_abandoned_uploads(max_age_seconds=7 * 24 * 3600)
    assert bad_id in deleted
    assert not d.exists()


def test_gc_orphan_sweep_spares_recent_corrupt_dir(manager):
    """A recently-created upload with a corrupt init.json must NOT be
    deleted — the recency gate keeps it alive until the next sweep."""
    bad_id = manager.create(MultipartInit(bucket="widgets", key="bad"))
    multipart_init_path(manager._data_dir, bad_id).write_text("{ corrupt")

    deleted = manager.gc_abandoned_uploads(max_age_seconds=7 * 24 * 3600)
    assert bad_id not in deleted
    assert multipart_dir(manager._data_dir, bad_id).exists()


def test_create_init_json_is_atomic(manager):
    """A successful create must leave a complete, parseable init.json
    and no leftover `.tmp` file — the contract of the atomic_write helper.
    """
    upload_id = manager.create(MultipartInit(bucket="widgets", key="k"))
    p = multipart_init_path(manager._data_dir, upload_id)
    with p.open(encoding="utf-8") as f:
        data = json.load(f)
    assert data["bucket"] == "widgets"
    assert data["key"] == "k"
    assert not p.with_suffix(p.suffix + ".tmp").exists()


def test_simulated_crash_mid_create_does_not_break_startup(manager):
    """If a crash leaves an upload dir with no init.json, the next
    startup's list_uploads must not raise and the GC orphan sweep
    must eventually reclaim the dir."""
    upload_id = manager.create(MultipartInit(bucket="widgets", key="k"))
    multipart_init_path(manager._data_dir, upload_id).unlink()

    assert all(uid != upload_id for uid, _ in manager.list_uploads())

    d = multipart_dir(manager._data_dir, upload_id)
    ancient_ts = (datetime.now(tz=UTC) - timedelta(days=30)).timestamp()
    os.utime(d, (ancient_ts, ancient_ts))

    deleted = manager.gc_abandoned_uploads(max_age_seconds=7 * 24 * 3600)
    assert upload_id in deleted
    assert not d.exists()


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


def test_concurrent_complete_does_not_corrupt(manager, storage):
    """Two concurrent Complete calls on the same uploadId must produce a
    valid object — never a corrupted concatenation.

    Regression for security audit finding H1: previously the scratch file
    `assembled.tmp` had a fixed name, so two parallel Completes would both
    open it with O_TRUNC, interleave their sendfile writes into the same
    inode, and rename the result to the final path. The "winning" rename
    moved a corrupted file into place. The fix names the scratch file
    uniquely per Complete invocation.

    We can't easily multi-thread the manager (it's not async), so this
    test launches multiple Complete operations concurrently via threads.
    """
    import hashlib
    import threading

    upload_id = manager.create(MultipartInit(bucket="widgets", key="big.bin"))
    part_a = b"A" * 1024
    part_b = b"B" * 1024
    p1 = asyncio.run(manager.upload_part(upload_id, 1, _stream(part_a)))
    p2 = asyncio.run(manager.upload_part(upload_id, 2, _stream(part_b)))
    expected = part_a + part_b
    expected_md5 = hashlib.md5(expected, usedforsecurity=False).hexdigest()

    # Run several Complete calls concurrently. They all reference the same
    # parts list, so the result must always be the byte-identical
    # concatenation. Some calls will fail with NoSuchUpload after a peer
    # has deleted the upload dir — that's fine, the survivors must all
    # produce the right object.
    errors: list[Exception] = []

    def go():
        try:
            manager.complete(upload_id, [(1, p1.etag), (2, p2.etag)])
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=go) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # The final object must exist and have the right content.
    head = storage.head_object("widgets", "big.bin")
    assert head.size == len(expected)

    async def _read():
        result = await storage.get_object("widgets", "big.bin")
        out = b""
        async for chunk in result.body:
            out += chunk
        return out

    body = asyncio.run(_read())
    assert body == expected
    assert hashlib.md5(body, usedforsecurity=False).hexdigest() == expected_md5
