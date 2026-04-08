"""Immutable runtime configuration for a nanio process."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from nanio.auth.credentials import CredentialResolver, EnvCredentialResolver

DEFAULT_DATA_DIR = Path("./nanio-data")
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 9000
DEFAULT_WORKERS = 1
DEFAULT_REGION = "us-east-1"
DEFAULT_MAX_LIST_KEYS = 1000
DEFAULT_LOG_LEVEL = "info"
DEFAULT_CHUNK_SIZE = 1024 * 1024  # 1 MiB streaming chunk


@dataclass(frozen=True, slots=True)
class Settings:
    """Process-wide settings. Immutable; pass through ASGI app state."""

    data_dir: Path = field(default_factory=lambda: DEFAULT_DATA_DIR.resolve())
    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    workers: int = DEFAULT_WORKERS
    region: str = DEFAULT_REGION
    max_list_keys: int = DEFAULT_MAX_LIST_KEYS
    log_level: str = DEFAULT_LOG_LEVEL
    chunk_size: int = DEFAULT_CHUNK_SIZE
    access_log: bool = True
    credentials: CredentialResolver = field(default_factory=EnvCredentialResolver)
    # Test-only escape hatch: when True, the auth middleware does NOT verify
    # SigV4 signatures. ONLY set from tests. Production code paths must leave
    # this False.
    auth_disabled: bool = False

    def __post_init__(self) -> None:
        if self.port < 1 or self.port > 65535:
            raise ValueError(f"port must be 1-65535, got {self.port}")
        if self.workers < 1:
            raise ValueError(f"workers must be >= 1, got {self.workers}")
        if self.max_list_keys < 1 or self.max_list_keys > 1000:
            raise ValueError(f"max_list_keys must be 1-1000, got {self.max_list_keys}")
        if self.chunk_size < 4096:
            raise ValueError(f"chunk_size must be >= 4096, got {self.chunk_size}")
