"""Targeted tests that close coverage gaps in small modules.

These are not feature tests — they exist to exercise the last few
branches that the broader wire-compat suite doesn't hit, so the project
stays at 100% coverage.

Grouped by the module under test.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from nanio import __version__
from nanio.auth.credentials import TomlFileCredentialResolver
from nanio.errors import (
    InvalidBucketName,
    InvalidObjectName,
    MalformedXML,
)
from nanio.handlers._body import parse_xml_safely
from nanio.keys import safe_join, validate_bucket_name, validate_object_key
from nanio.storage.paths import (
    assert_inside_data_dir,
    atomic_write,
    multipart_dir,
    multipart_part_md5_path,
    multipart_part_path,
)
from nanio.xml import list_objects_v2_xml, list_parts_xml

# ----------------------------------------------------------------------
# nanio/__init__.py — PackageNotFoundError branch
# ----------------------------------------------------------------------


def test_package_version_is_a_string():
    """The __version__ constant must exist and be a string regardless of
    whether the package metadata was discoverable at import time."""
    assert isinstance(__version__, str)
    assert __version__  # non-empty


def test_package_version_fallback_when_metadata_missing():
    """If `importlib.metadata.version` raises, `__version__` falls back
    to a sentinel."""
    import importlib
    import importlib.metadata

    import nanio

    def raise_notfound(name):
        raise importlib.metadata.PackageNotFoundError(name)

    with patch.object(importlib.metadata, "version", side_effect=raise_notfound):
        # Force a re-import of the module so the module-level try/except
        # runs against our patched version().
        reloaded = importlib.reload(nanio)
    assert reloaded.__version__ == "0.0.0+local"

    # Restore real version so subsequent tests see a real string.
    importlib.reload(nanio)


# ----------------------------------------------------------------------
# nanio/app.py — _settings_from_env + factory mode
# ----------------------------------------------------------------------


def test_build_app_factory_mode_reads_settings_from_env(tmp_path):
    """When build_app is called with no settings, it reconstructs them
    from env vars — exactly as uvicorn's factory mode does."""
    from nanio.app import _settings_from_env, build_app

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    env = {
        "NANIO_DATA_DIR": str(data_dir),
        "NANIO_REGION": "eu-west-1",
        "NANIO_ACCESS_KEY": "ak",
        "NANIO_SECRET_KEY": "sk",
    }
    with patch.dict(os.environ, env, clear=True):
        settings = _settings_from_env()
        assert settings.data_dir == data_dir
        assert settings.region == "eu-west-1"
        assert settings.credentials.resolve("ak") == "sk"

        app = build_app()
        assert app.state.settings.region == "eu-west-1"


def test_build_app_factory_mode_loads_options_file(tmp_path):
    """If NANIO_OPTIONS_FILE is set, the factory uses the TOML resolver."""
    from nanio.app import _settings_from_env

    options_file = tmp_path / "options.toml"
    options_file.write_text('[[users]]\naccess_key = "alice"\nsecret_key = "wonderland"\n')
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    env = {
        "NANIO_DATA_DIR": str(data_dir),
        "NANIO_OPTIONS_FILE": str(options_file),
    }
    with patch.dict(os.environ, env, clear=True):
        settings = _settings_from_env()
        assert settings.credentials.resolve("alice") == "wonderland"


# ----------------------------------------------------------------------
# nanio/auth/credentials.py — duplicate access key branch
# ----------------------------------------------------------------------


def test_toml_resolver_rejects_non_table_users_entry(tmp_path):
    p = tmp_path / "creds.toml"
    p.write_text('users = ["not a table"]\n')
    with pytest.raises(ValueError, match="must be a table"):
        TomlFileCredentialResolver(p)


# ----------------------------------------------------------------------
# nanio/keys.py — validator error branches
# ----------------------------------------------------------------------


def test_validate_bucket_name_rejects_non_string():
    with pytest.raises(InvalidBucketName, match="must be a string"):
        validate_bucket_name(123)  # type: ignore[arg-type]


def test_validate_object_key_rejects_non_string():
    with pytest.raises(InvalidObjectName, match="must be a string"):
        validate_object_key(42)  # type: ignore[arg-type]


def test_validate_bucket_name_with_consecutive_dots():
    # Already in test_keys but check the `..` branch explicitly for coverage.
    with pytest.raises(InvalidBucketName):
        validate_bucket_name("abc..def")


def test_safe_join_rejects_backslash_dotdot(tmp_path):
    """The `..` rejection loop splits on both `/` and `\\`."""
    with pytest.raises(ValueError, match="refused"):
        safe_join(tmp_path.resolve(), "bucket", "a\\..\\b")


# ----------------------------------------------------------------------
# nanio/xml.py — StartAfter + NextPartMarker branches
# ----------------------------------------------------------------------


def test_list_objects_v2_xml_with_start_after():
    """The `start_after is not None` branch."""
    when = datetime(2026, 1, 1, tzinfo=UTC)
    xml = list_objects_v2_xml(
        bucket="widgets",
        contents=[("foo.txt", when, '"etag"', 1)],
        common_prefixes=[],
        start_after="previous-key.txt",
    )
    assert b"<StartAfter>previous-key.txt</StartAfter>" in xml


def test_list_parts_xml_with_next_part_marker():
    """The `next_part_marker is not None` branch."""
    when = datetime(2026, 1, 1, tzinfo=UTC)
    xml = list_parts_xml(
        bucket="b",
        key="k",
        upload_id="u",
        parts_listed=[(1, when, '"a"', 1024)],
        next_part_marker=42,
    )
    assert b"<NextPartNumberMarker>42</NextPartNumberMarker>" in xml


# ----------------------------------------------------------------------
# nanio/handlers/_body.py — empty body branch
# ----------------------------------------------------------------------


def test_parse_xml_safely_rejects_empty_body():
    with pytest.raises(MalformedXML, match="empty"):
        parse_xml_safely(b"")


# ----------------------------------------------------------------------
# nanio/storage/paths.py — multipart paths + atomic_write error paths
# ----------------------------------------------------------------------


def test_multipart_dir_rejects_invalid_upload_id(tmp_path):
    with pytest.raises(ValueError):
        multipart_dir(tmp_path, "a/b")
    with pytest.raises(ValueError):
        multipart_dir(tmp_path, "..escape")


def test_multipart_part_path_rejects_invalid_part_number(tmp_path):
    with pytest.raises(ValueError):
        multipart_part_path(tmp_path, "u", 0)
    with pytest.raises(ValueError):
        multipart_part_md5_path(tmp_path, "u", 10_001)


def test_atomic_write_cleans_up_on_exception(tmp_path):
    """If the caller raises inside the `atomic_write` block, the tmp file
    must be unlinked and the exception re-raised."""
    target = tmp_path / "out.bin"

    class Boom(RuntimeError):
        pass

    with pytest.raises(Boom), atomic_write(target) as fd:
        os.write(fd, b"partial")
        raise Boom()

    assert not target.exists()
    # No stale .tmp file left behind.
    assert not any(tmp_path.glob("*.tmp"))


def test_atomic_write_commit_is_atomic(tmp_path):
    """Happy-path commit leaves only the final file, not the tmp."""
    target = tmp_path / "sub" / "out.bin"
    target.parent.mkdir()
    with atomic_write(target) as fd:
        os.write(fd, b"committed")
    assert target.read_bytes() == b"committed"
    assert not any(target.parent.glob("*.tmp"))


def test_assert_inside_data_dir_raises_for_target_outside(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    with pytest.raises(PermissionError, match="outside"):
        assert_inside_data_dir(data_dir, outside)


def test_assert_inside_data_dir_accepts_target_inside(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    inside = data_dir / "nested" / "file.txt"
    # Doesn't need to exist — realpath handles nonexistent paths.
    assert_inside_data_dir(data_dir, inside)


def test_assert_inside_data_dir_rejects_commonpath_value_error(tmp_path):
    """Force `os.path.commonpath` to raise by passing paths on different
    absolute roots (hard on posix, but the ValueError branch is
    reachable if Path handling falls over)."""
    data_dir = Path("/some/abs/path")
    with pytest.raises(PermissionError):
        assert_inside_data_dir(data_dir, Path("//other/root"))
