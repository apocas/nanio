"""Tests for nanio.storage.paths."""

from __future__ import annotations

import pytest

from nanio.storage.paths import (
    META_DIR_NAME,
    bucket_dir,
    is_internal_name,
    metadata_path,
    multipart_dir,
    multipart_init_path,
    multipart_part_md5_path,
    multipart_part_path,
    multipart_root,
    object_path,
)


def test_bucket_dir(tmp_path):
    assert bucket_dir(tmp_path, "widgets") == tmp_path / "widgets"


def test_object_path_simple(tmp_path):
    p = object_path(tmp_path, "widgets", "foo.txt")
    assert p == tmp_path / "widgets" / "foo.txt"


def test_object_path_with_subdir(tmp_path):
    p = object_path(tmp_path, "widgets", "a/b/c.txt")
    assert p == tmp_path / "widgets" / "a" / "b" / "c.txt"


def test_object_path_rejects_dotdot(tmp_path):
    with pytest.raises(ValueError):
        object_path(tmp_path, "widgets", "../escape.txt")


def test_metadata_path(tmp_path):
    p = metadata_path(tmp_path, "widgets", "a/b/c.txt")
    assert p == tmp_path / "widgets" / META_DIR_NAME / "a" / "b" / "c.txt.json"


def test_multipart_paths(tmp_path):
    upload_id = "abc123"
    assert multipart_root(tmp_path) == tmp_path / ".nanio" / "multipart"
    assert multipart_dir(tmp_path, upload_id) == tmp_path / ".nanio" / "multipart" / upload_id
    assert multipart_init_path(tmp_path, upload_id).name == "init.json"
    assert multipart_part_path(tmp_path, upload_id, 1).name == "000001.bin"
    assert multipart_part_path(tmp_path, upload_id, 9999).name == "009999.bin"
    assert multipart_part_md5_path(tmp_path, upload_id, 5).name == "000005.md5"


def test_multipart_dir_rejects_bad_upload_id(tmp_path):
    with pytest.raises(ValueError):
        multipart_dir(tmp_path, "../escape")
    with pytest.raises(ValueError):
        multipart_dir(tmp_path, "a/b")


def test_multipart_part_number_bounds(tmp_path):
    with pytest.raises(ValueError):
        multipart_part_path(tmp_path, "u", 0)
    with pytest.raises(ValueError):
        multipart_part_path(tmp_path, "u", 10001)


def test_is_internal_name():
    assert is_internal_name(".nanio")
    assert is_internal_name(".nanio-meta")
    assert is_internal_name(".nanio-multipart")
    assert not is_internal_name("regular-bucket")
    assert not is_internal_name("foo.txt")
