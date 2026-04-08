"""Tests for nanio.config.Settings validation."""

from __future__ import annotations

import dataclasses
import os
from unittest.mock import patch

import pytest

from nanio.auth.credentials import StaticCredentialResolver
from nanio.config import Settings


@pytest.fixture
def env_creds():
    with patch.dict(os.environ, {"NANIO_ACCESS_KEY": "ak", "NANIO_SECRET_KEY": "sk"}):
        yield


def test_defaults(env_creds, tmp_path):
    s = Settings(data_dir=tmp_path)
    assert s.data_dir == tmp_path
    assert s.host == "0.0.0.0"
    assert s.port == 9000
    assert s.workers == 1
    assert s.region == "us-east-1"
    assert s.max_list_keys == 1000
    assert s.chunk_size == 1024 * 1024
    assert s.access_log is True
    assert s.auth_disabled is False


def test_settings_is_immutable(env_creds, tmp_path):
    s = Settings(data_dir=tmp_path)
    with pytest.raises(dataclasses.FrozenInstanceError):
        s.port = 8080  # type: ignore[misc]


def test_invalid_port(env_creds, tmp_path):
    with pytest.raises(ValueError, match="port must be"):
        Settings(data_dir=tmp_path, port=0)
    with pytest.raises(ValueError, match="port must be"):
        Settings(data_dir=tmp_path, port=70000)


def test_invalid_workers(env_creds, tmp_path):
    with pytest.raises(ValueError, match="workers must be"):
        Settings(data_dir=tmp_path, workers=0)


def test_invalid_max_list_keys(env_creds, tmp_path):
    with pytest.raises(ValueError, match="max_list_keys"):
        Settings(data_dir=tmp_path, max_list_keys=0)
    with pytest.raises(ValueError, match="max_list_keys"):
        Settings(data_dir=tmp_path, max_list_keys=1001)


def test_invalid_chunk_size(env_creds, tmp_path):
    with pytest.raises(ValueError, match="chunk_size"):
        Settings(data_dir=tmp_path, chunk_size=1024)


def test_explicit_credential_resolver(tmp_path):
    creds = StaticCredentialResolver({"alice": "secret"})
    s = Settings(data_dir=tmp_path, credentials=creds)
    assert s.credentials.resolve("alice") == "secret"
    assert s.credentials.resolve("bob") is None
