"""TOML options-file loader for `nanio serve`.

The options file is the persistent on-disk config that pairs with
`nanio install`. It carries server tunables under a `[server]` table
and credentials under `[[users]]` (consumed separately by
`nanio.auth.credentials.TomlFileCredentialResolver`).

Example:

    # /etc/nanio/options.toml
    [server]
    data_dir = "/var/lib/nanio"
    host     = "0.0.0.0"
    port     = 9000
    region   = "us-east-1"
    workers  = 1
    log_level = "info"

    [[users]]
    access_key = "AKIDEXAMPLE"
    secret_key = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"

CLI flags and env vars override the options file. The resolution
order in `nanio.cli._cmd_serve` is:

    CLI flag > env var > options file > built-in default
"""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any

DEFAULT_OPTIONS_PATH = Path("/etc/nanio/options.toml")

#: Server-tunable keys recognised in the `[server]` section. Anything
#: else triggers a load-time error so typos can't silently no-op.
SERVER_KEYS: frozenset[str] = frozenset(
    {
        "data_dir",
        "host",
        "port",
        "region",
        "workers",
        "log_level",
    }
)


def load_server_options(path: Path) -> dict[str, Any]:
    """Load and validate the `[server]` table from an options file.

    Returns an empty dict if the file has no `[server]` section.
    Raises `FileNotFoundError` if the file doesn't exist, `ValueError`
    on malformed TOML or unknown server keys.
    """
    if not path.is_file():
        raise FileNotFoundError(f"options file not found: {path}")
    with path.open("rb") as f:
        try:
            data = tomllib.load(f)
        except tomllib.TOMLDecodeError as exc:
            raise ValueError(f"options file {path} is not valid TOML: {exc}") from exc
    server = data.get("server", {})
    if not isinstance(server, dict):
        raise ValueError(f"[server] in {path} must be a table, got {type(server).__name__}")
    unknown = set(server) - SERVER_KEYS
    if unknown:
        raise ValueError(
            f"unknown keys in [server] of {path}: {sorted(unknown)} "
            f"(allowed: {sorted(SERVER_KEYS)})"
        )
    return server
