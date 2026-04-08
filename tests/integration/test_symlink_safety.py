"""Regression tests for symlink safety in the storage layer (H5).

If an attacker can pre-place a symlink under `--data-dir` (realistic on
shared NFS, multi-tenant hosts, or alongside another service that can
write into the data dir), they could:

- GET an object whose key resolves to a leaf symlink → /etc/passwd → read it
- PUT through a parent symlink → write attacker bytes outside the data dir
- Multipart Complete via a sidecar symlink → ditto

The fix is `O_NOFOLLOW` on every storage open, plus `assert_inside_data_dir`
on parent dirs that the storage layer is about to mkdir into. These tests
exercise both layers.
"""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime

import pytest

from nanio.errors import NoSuchBucket, NoSuchKey
from nanio.storage.filesystem import FilesystemStorage
from nanio.storage.metadata import synthesize_metadata_from_stat
from nanio.storage.paths import metadata_path

# These tests use the storage backend directly so we don't need to spin up
# the whole ASGI app — the threat model is at the filesystem layer.


@pytest.fixture
def storage(tmp_path):
    s = FilesystemStorage(tmp_path)
    s.create_bucket("widgets")
    return s


async def _stream(data: bytes):
    yield data


def test_get_through_leaf_symlink_refused(storage, tmp_path):
    """A symlink at <bucket>/key must NOT be readable via GetObject."""
    target = tmp_path / "secret.txt"
    target.write_text("super secret")
    link = tmp_path / "widgets" / "smuggled"
    link.symlink_to(target)

    with pytest.raises(NoSuchKey):
        storage.head_object("widgets", "smuggled")

    with pytest.raises(NoSuchKey):
        asyncio.run(storage.get_object("widgets", "smuggled"))


def test_get_through_intermediate_symlink_refused(storage, tmp_path):
    """A symlinked subdir that escapes the data dir must not be readable.

    The realpath check (`assert_inside_data_dir`) catches symlinks at any
    intermediate component, not just the leaf.
    """
    # Place a symlink dir inside the bucket pointing OUTSIDE the data dir.
    intermediate = tmp_path / "widgets" / "esc"
    decoy_target_dir = tmp_path.parent / "outside-data-dir"
    decoy_target_dir.mkdir(exist_ok=True)
    (decoy_target_dir / "passwd").write_text("not real passwd")
    intermediate.symlink_to(decoy_target_dir)

    # The escaped read should not succeed via head_object — the realpath
    # check sees that the resolved path is outside `data_dir`.
    with pytest.raises(NoSuchKey):
        storage.head_object("widgets", "esc/passwd")


def test_put_through_parent_symlink_refused(storage, tmp_path):
    """A pre-placed symlinked subdir under the bucket must not be a write target.

    The fix is `assert_inside_data_dir` on the parent before mkdir + open;
    the symlinked dir realpaths to outside the data root.
    """
    outside = tmp_path.parent / "elsewhere"
    outside.mkdir(exist_ok=True)
    link_dir = tmp_path / "widgets" / "evil"
    link_dir.symlink_to(outside)

    with pytest.raises(PermissionError):
        asyncio.run(storage.put_object("widgets", "evil/foo.txt", _stream(b"bytes")))

    # The outside dir must NOT contain the file.
    assert not (outside / "foo.txt").exists()


def test_put_then_replaced_by_symlink_does_not_redirect_metadata(storage, tmp_path):
    """If an attacker replaces the metadata sidecar with a symlink between
    PUT and the next read, O_NOFOLLOW must refuse to read it and
    head_object must fall back to synthesized metadata from the real
    object file."""
    asyncio.run(storage.put_object("widgets", "k.txt", _stream(b"hello")))

    sidecar = metadata_path(tmp_path, "widgets", "k.txt")
    sidecar.unlink()
    fake_target = tmp_path / "fake-meta.json"
    fake_target.write_text(
        '{"etag":"\\"deadbeef\\"","size":99999,"last_modified":"2026-01-01T00:00:00+00:00"}'
    )
    sidecar.symlink_to(fake_target)

    # Real file is 5 bytes; the symlinked sidecar falsely claims 99999.
    info = storage.head_object("widgets", "k.txt")
    assert info.size == 5


def test_listing_skips_symlinked_objects(storage, tmp_path):
    """A symlinked object that resolves outside the data dir should not
    appear in listings, OR if it does, must not allow read."""
    asyncio.run(storage.put_object("widgets", "real.txt", _stream(b"real")))
    target = tmp_path.parent / "outside.txt"
    target.write_text("escaped")
    link = tmp_path / "widgets" / "fake.txt"
    link.symlink_to(target)

    result = storage.list_objects("widgets")
    keys = {c.key for c in result.contents}
    # Either the symlink is filtered out OR it appears but a subsequent
    # GET refuses to read it.
    if "fake.txt" in keys:
        with pytest.raises(NoSuchKey):
            storage.head_object("widgets", "fake.txt")
    assert "real.txt" in keys


def test_symlinked_bucket_root_skipped_from_list_buckets(tmp_path):
    storage = FilesystemStorage(tmp_path)
    storage.create_bucket("widgets")

    outside = tmp_path.parent / "outside-bucket-list"
    outside.mkdir(exist_ok=True)
    (outside / "leaked.txt").write_text("escaped")
    (tmp_path / "escaped-bucket").symlink_to(outside)

    names = [bucket.name for bucket in storage.list_buckets()]
    assert "widgets" in names
    assert "escaped-bucket" not in names


def test_symlinked_bucket_root_refused_for_bucket_operations(tmp_path):
    storage = FilesystemStorage(tmp_path)

    outside = tmp_path.parent / "outside-bucket-ops"
    outside.mkdir(exist_ok=True)
    (outside / "leaked.txt").write_text("escaped")
    (tmp_path / "escaped-root").symlink_to(outside)

    with pytest.raises(NoSuchBucket):
        storage.head_bucket("escaped-root")

    with pytest.raises(NoSuchBucket):
        storage.list_objects("escaped-root")


def test_synthesize_metadata_does_not_follow_symlink(tmp_path):
    """`synthesize_metadata_from_stat` must lstat, not stat — otherwise
    a symlinked path in the fallback leaks the target file's size and
    mtime through the listing."""
    canary = tmp_path.parent / "security-canary-h1v2.bin"
    canary.write_bytes(b"x" * 7777)
    os.utime(canary, (1, 1))

    sym_path = tmp_path / "widgets-not-a-bucket"
    sym_path.symlink_to(canary)

    info = synthesize_metadata_from_stat(sym_path, "sym")

    assert info.size != 7777  # lstat of the symlink, not the 7777-byte target
    assert info.last_modified > datetime(2000, 1, 1, tzinfo=UTC)  # not 1970
