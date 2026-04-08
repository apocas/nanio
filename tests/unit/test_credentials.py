"""Tests for nanio.auth.credentials resolvers."""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from nanio.auth.credentials import (
    EnvCredentialResolver,
    StaticCredentialResolver,
    TomlFileCredentialResolver,
)


def test_static_resolver_lookup():
    r = StaticCredentialResolver({"alice": "s1", "bob": "s2"})
    assert r.resolve("alice") == "s1"
    assert r.resolve("bob") == "s2"
    assert r.resolve("eve") is None
    assert r.access_keys() == ["alice", "bob"]


def test_static_resolver_rejects_empty():
    with pytest.raises(ValueError):
        StaticCredentialResolver({})


def test_static_resolver_rejects_blank_pair():
    with pytest.raises(ValueError):
        StaticCredentialResolver({"": "secret"})
    with pytest.raises(ValueError):
        StaticCredentialResolver({"alice": ""})


def test_env_resolver_happy_path():
    with patch.dict(os.environ, {"NANIO_ACCESS_KEY": "ak", "NANIO_SECRET_KEY": "sk"}):
        r = EnvCredentialResolver()
        assert r.resolve("ak") == "sk"
        assert r.resolve("other") is None


def test_env_resolver_missing_raises():
    with (
        patch.dict(os.environ, {}, clear=True),
        pytest.raises(ValueError, match="NANIO_ACCESS_KEY"),
    ):
        EnvCredentialResolver()


def test_toml_resolver(tmp_path):
    p = tmp_path / "creds.toml"
    p.write_text(
        """
[[users]]
access_key = "alice"
secret_key = "s1"

[[users]]
access_key = "bob"
secret_key = "s2"
"""
    )
    r = TomlFileCredentialResolver(p)
    assert r.resolve("alice") == "s1"
    assert r.resolve("bob") == "s2"
    assert r.access_keys() == ["alice", "bob"]


def test_toml_resolver_missing_file(tmp_path):
    with pytest.raises(FileNotFoundError):
        TomlFileCredentialResolver(tmp_path / "nope.toml")


def test_toml_resolver_no_users(tmp_path):
    p = tmp_path / "creds.toml"
    p.write_text("# empty file\n")
    with pytest.raises(ValueError, match="users"):
        TomlFileCredentialResolver(p)


def test_toml_resolver_duplicate_access_key(tmp_path):
    p = tmp_path / "creds.toml"
    p.write_text(
        """
[[users]]
access_key = "dup"
secret_key = "s1"
[[users]]
access_key = "dup"
secret_key = "s2"
"""
    )
    with pytest.raises(ValueError, match="duplicate"):
        TomlFileCredentialResolver(p)


def test_toml_resolver_bad_field_types(tmp_path):
    p = tmp_path / "creds.toml"
    p.write_text(
        """
[[users]]
access_key = 123
secret_key = "x"
"""
    )
    with pytest.raises(ValueError):
        TomlFileCredentialResolver(p)
