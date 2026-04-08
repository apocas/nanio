"""Credential resolvers — pluggable mapping from access_key to secret_key.

The default `EnvCredentialResolver` reads `NANIO_ACCESS_KEY` and
`NANIO_SECRET_KEY` once at startup and serves a single root user. The
`TomlFileCredentialResolver` loads multiple users from a TOML file:

    # nanio-credentials.toml
    [[users]]
    access_key = "minioadmin"
    secret_key = "minioadmin"

    [[users]]
    access_key = "alice"
    secret_key = "..."

Resolvers are immutable after construction and safe to share across worker
processes (they only do dict lookups, no I/O on the request path).
"""

from __future__ import annotations

import os
import tomllib
from pathlib import Path
from typing import Protocol


class CredentialResolver(Protocol):
    """Pluggable lookup of secret keys by access key."""

    def resolve(self, access_key: str) -> str | None:
        """Return the secret key for the given access key, or None if unknown."""

    def access_keys(self) -> list[str]:
        """Return all known access keys (used for startup validation only)."""


class StaticCredentialResolver:
    """Resolver backed by a fixed dict. The base implementation."""

    __slots__ = ("_pairs",)

    def __init__(self, pairs: dict[str, str]) -> None:
        if not pairs:
            raise ValueError("StaticCredentialResolver needs at least one credential pair")
        for k, v in pairs.items():
            if not k or not v:
                raise ValueError("access_key and secret_key must both be non-empty")
        self._pairs = dict(pairs)

    def resolve(self, access_key: str) -> str | None:
        return self._pairs.get(access_key)

    def access_keys(self) -> list[str]:
        return sorted(self._pairs)


class EnvCredentialResolver(StaticCredentialResolver):
    """Resolver that reads `NANIO_ACCESS_KEY`/`NANIO_SECRET_KEY` from env vars."""

    def __init__(self) -> None:
        access = os.environ.get("NANIO_ACCESS_KEY")
        secret = os.environ.get("NANIO_SECRET_KEY")
        if not access or not secret:
            raise ValueError(
                "NANIO_ACCESS_KEY and NANIO_SECRET_KEY must both be set "
                "in the environment, or provide a --credentials-file."
            )
        super().__init__({access: secret})


class TomlFileCredentialResolver(StaticCredentialResolver):
    """Resolver loaded from a TOML file at startup. File is read once."""

    def __init__(self, path: str | os.PathLike[str]) -> None:
        p = Path(path)
        if not p.is_file():
            raise FileNotFoundError(f"credentials file not found: {p}")
        with p.open("rb") as f:
            data = tomllib.load(f)
        users = data.get("users", [])
        if not isinstance(users, list) or not users:
            raise ValueError(f"credentials file {p} must contain at least one [[users]] table")
        pairs: dict[str, str] = {}
        for i, entry in enumerate(users):
            if not isinstance(entry, dict):
                raise ValueError(f"users[{i}] must be a table, got {type(entry).__name__}")
            access = entry.get("access_key")
            secret = entry.get("secret_key")
            if not isinstance(access, str) or not isinstance(secret, str):
                raise ValueError(f"users[{i}] must have string access_key and secret_key fields")
            if access in pairs:
                raise ValueError(f"duplicate access_key in credentials file: {access!r}")
            pairs[access] = secret
        super().__init__(pairs)
