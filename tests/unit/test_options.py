"""Tests for the `nanio.options` TOML loader."""

from __future__ import annotations

import pytest

from nanio.options import (
    DEFAULT_OPTIONS_PATH,
    SERVER_KEYS,
    load_server_options,
)


def test_default_options_path_constant():
    assert DEFAULT_OPTIONS_PATH.name == "options.toml"
    assert DEFAULT_OPTIONS_PATH.parent.name == "nanio"


def test_load_full_server_section(tmp_path):
    p = tmp_path / "options.toml"
    p.write_text(
        """
[server]
data_dir = "/var/lib/nanio"
host = "127.0.0.1"
port = 9001
region = "eu-west-1"
workers = 4
log_level = "warning"

[[users]]
access_key = "alice"
secret_key = "wonderland"
"""
    )
    server = load_server_options(p)
    assert server == {
        "data_dir": "/var/lib/nanio",
        "host": "127.0.0.1",
        "port": 9001,
        "region": "eu-west-1",
        "workers": 4,
        "log_level": "warning",
    }


def test_load_returns_empty_when_no_server_section(tmp_path):
    """A file with only [[users]] (no [server]) returns {}."""
    p = tmp_path / "options.toml"
    p.write_text(
        """
[[users]]
access_key = "ak"
secret_key = "sk"
"""
    )
    assert load_server_options(p) == {}


def test_load_partial_server_section(tmp_path):
    """Only the keys present in the file are returned."""
    p = tmp_path / "options.toml"
    p.write_text("""[server]\nport = 8080\n""")
    assert load_server_options(p) == {"port": 8080}


def test_load_missing_file_raises_filenotfound(tmp_path):
    with pytest.raises(FileNotFoundError, match="not found"):
        load_server_options(tmp_path / "nope.toml")


def test_load_malformed_toml_raises_value_error(tmp_path):
    p = tmp_path / "options.toml"
    p.write_text("not valid toml [[[")
    with pytest.raises(ValueError, match="not valid TOML"):
        load_server_options(p)


def test_load_server_not_a_table_raises(tmp_path):
    p = tmp_path / "options.toml"
    p.write_text('server = "oops"\n')
    with pytest.raises(ValueError, match="must be a table"):
        load_server_options(p)


def test_load_unknown_server_key_raises(tmp_path):
    p = tmp_path / "options.toml"
    p.write_text(
        """
[server]
data_dir = "/var/lib/nanio"
unknown_field = "oops"
"""
    )
    with pytest.raises(ValueError, match="unknown keys"):
        load_server_options(p)


def test_recognised_server_keys_set():
    """Lock the public set of recognised server keys."""
    assert frozenset({"data_dir", "host", "port", "region", "workers", "log_level"}) == SERVER_KEYS
