"""Tests for nanio.storage.metadata sidecar JSON read/write."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from nanio.storage.backend import ObjectInfo
from nanio.storage.metadata import (
    read_metadata,
    synthesize_metadata_from_stat,
    write_metadata,
)


def _info(**overrides) -> ObjectInfo:
    base = dict(
        key="path/to/file.txt",
        size=42,
        etag='"deadbeef"',
        last_modified=datetime(2026, 4, 8, 12, 0, 0, tzinfo=UTC),
        content_type="text/plain",
        user_metadata={"foo": "bar"},
    )
    base.update(overrides)
    return ObjectInfo(**base)


def test_round_trip(tmp_path):
    p = tmp_path / "meta.json"
    info = _info()
    write_metadata(p, info)
    loaded = read_metadata(p, info.key)
    assert loaded.key == info.key
    assert loaded.size == info.size
    assert loaded.etag == info.etag
    assert loaded.content_type == info.content_type
    assert loaded.user_metadata == info.user_metadata
    assert loaded.last_modified == info.last_modified


def test_write_creates_parent_dirs(tmp_path):
    p = tmp_path / "deeply" / "nested" / "meta.json"
    write_metadata(p, _info())
    assert p.is_file()


def test_write_is_atomic_no_tmp_left_over(tmp_path):
    p = tmp_path / "meta.json"
    write_metadata(p, _info())
    leftovers = list(tmp_path.glob("*.tmp"))
    assert not leftovers


def test_read_missing_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        read_metadata(tmp_path / "nope.json", "k")


def test_synthesize_from_stat(tmp_path):
    f = tmp_path / "blob"
    f.write_bytes(b"hello world")
    info = synthesize_metadata_from_stat(f, "k")
    assert info.size == 11
    assert info.etag == '""'
    assert info.content_type == "application/octet-stream"
