"""Command-line entry point for nanio.

- `nanio serve` — runs the HTTP server (delegates to uvicorn).
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Any

from nanio import __version__
from nanio import options as _options

log = logging.getLogger("nanio.cli")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="nanio",
        description="A minimal, stateless, S3-compatible object storage server.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"nanio {__version__}",
    )
    parser.set_defaults(func=None)

    subparsers = parser.add_subparsers(title="commands", dest="command")

    serve = subparsers.add_parser(
        "serve",
        help="Run the nanio HTTP server.",
        description=(
            "Run the nanio HTTP server. Configuration sources (in precedence "
            "order): CLI flags > NANIO_* env vars > options file > defaults. "
            "Either --options or NANIO_ACCESS_KEY/NANIO_SECRET_KEY env vars "
            "must supply credentials."
        ),
    )
    serve.add_argument(
        "--options",
        type=Path,
        default=None,
        help=(
            "TOML options file with [server] tunables and [[users]] credentials "
            "(env: NANIO_OPTIONS_FILE)."
        ),
    )
    serve.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="Root directory for bucket data (default: ./nanio-data, env: NANIO_DATA_DIR)",
    )
    serve.add_argument(
        "--host",
        default=None,
        help="Bind host (default: 0.0.0.0, env: NANIO_HOST)",
    )
    serve.add_argument(
        "--port",
        type=int,
        default=None,
        help="Bind port (default: 9000, env: NANIO_PORT)",
    )
    serve.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Number of uvicorn worker processes (default: 1, env: NANIO_WORKERS)",
    )
    serve.add_argument(
        "--region",
        default=None,
        help="S3 region to report (default: us-east-1, env: NANIO_REGION)",
    )
    serve.add_argument(
        "--log-level",
        choices=["debug", "info", "warning", "error"],
        default=None,
        help="Log verbosity (default: info, env: NANIO_LOG_LEVEL)",
    )
    serve.add_argument(
        "--no-access-log",
        dest="access_log",
        action="store_false",
        default=True,
        help="Disable per-request access logs",
    )
    serve.add_argument(
        "--gc-abandoned-uploads",
        action="store_true",
        default=False,
        help=(
            "Delete (instead of just warn about) multipart uploads older than "
            "7 days at startup. Use this on a periodic schedule (cron, systemd "
            "timer) on the host that owns the data dir."
        ),
    )
    serve.set_defaults(func=_cmd_serve)

    return parser


# ----------------------------------------------------------------------
# Precedence helpers — CLI flag > env var > options file > default
# ----------------------------------------------------------------------


def _resolve_str(
    cli_val: str | None,
    env_var: str,
    options: dict[str, Any],
    key: str,
    default: str,
) -> str:
    if cli_val is not None:
        return cli_val
    env_val = os.environ.get(env_var)
    if env_val is not None:
        return env_val
    if key in options:
        return str(options[key])
    return default


def _resolve_int(
    cli_val: int | None,
    env_var: str,
    options: dict[str, Any],
    key: str,
    default: int,
) -> int:
    if cli_val is not None:
        return cli_val
    env_val = os.environ.get(env_var)
    if env_val is not None:
        return int(env_val)
    if key in options:
        return int(options[key])
    return default


def _resolve_path(
    cli_val: Path | None,
    env_var: str,
    options: dict[str, Any],
    key: str,
    default: Path,
) -> Path:
    if cli_val is not None:
        return cli_val
    env_val = os.environ.get(env_var)
    if env_val is not None:
        return Path(env_val)
    if key in options:
        return Path(str(options[key]))
    return default


def _cmd_serve(args: argparse.Namespace) -> int:
    import uvicorn

    from nanio.auth.credentials import (
        CredentialResolver,
        EnvCredentialResolver,
        TomlFileCredentialResolver,
    )
    from nanio.config import DEFAULT_DATA_DIR, Settings
    from nanio.logging import setup_logging

    # Locate and load the options file (CLI flag wins, then env var).
    options_path = args.options or _path_or_none(os.environ.get("NANIO_OPTIONS_FILE"))
    options: dict[str, Any] = {}
    if options_path is not None:
        try:
            options = _options.load_server_options(options_path)
        except (FileNotFoundError, ValueError) as exc:
            print(f"nanio: failed to load options file: {exc}", file=sys.stderr)
            return 2

    log_level = _resolve_str(args.log_level, "NANIO_LOG_LEVEL", options, "log_level", "info")
    setup_logging(log_level)

    # Credentials: prefer the options file (its [[users]] table), fall
    # back to env vars.
    credentials: CredentialResolver
    if options_path is not None:
        try:
            credentials = TomlFileCredentialResolver(options_path)
        except (FileNotFoundError, ValueError) as exc:
            print(f"nanio: failed to load credentials from options file: {exc}", file=sys.stderr)
            return 2
    else:
        try:
            credentials = EnvCredentialResolver()
        except ValueError as exc:
            print(f"nanio: {exc}", file=sys.stderr)
            return 2

    data_dir = _resolve_path(
        args.data_dir, "NANIO_DATA_DIR", options, "data_dir", DEFAULT_DATA_DIR
    ).resolve()
    host = _resolve_str(args.host, "NANIO_HOST", options, "host", "0.0.0.0")
    port = _resolve_int(args.port, "NANIO_PORT", options, "port", 9000)
    workers = _resolve_int(args.workers, "NANIO_WORKERS", options, "workers", 1)
    region = _resolve_str(args.region, "NANIO_REGION", options, "region", "us-east-1")

    settings = Settings(
        data_dir=data_dir,
        host=host,
        port=port,
        workers=workers,
        region=region,
        credentials=credentials,
        log_level=log_level,
        access_log=args.access_log,
    )

    # Persist resolved values into env vars so worker processes (which
    # re-import the app via the uvicorn factory) construct an identical
    # Settings instance without re-reading the options file.
    os.environ["NANIO_DATA_DIR"] = str(data_dir)
    os.environ["NANIO_REGION"] = region
    if options_path is not None:
        os.environ["NANIO_OPTIONS_FILE"] = str(options_path)

    log.info(
        "nanio %s starting on %s:%d (data-dir=%s, workers=%d)",
        __version__,
        settings.host,
        settings.port,
        settings.data_dir,
        settings.workers,
    )

    # Handle abandoned multipart uploads at startup.
    #
    # Without --gc-abandoned-uploads we only WARN — operators may want to
    # inspect the upload dirs first. With the flag, we delete them on the
    # spot. Either way the scan is best-effort and never aborts startup.
    try:
        from nanio.storage.multipart import MultipartManager

        manager = MultipartManager(data_dir)
        if args.gc_abandoned_uploads:
            deleted = manager.gc_abandoned_uploads()
            if deleted:
                log.warning(
                    "deleted %d abandoned multipart upload(s) older than 7 days under %s",
                    len(deleted),
                    data_dir,
                )
        else:
            old = manager.warn_about_abandoned_uploads()
            if old:
                log.warning(
                    "%d abandoned multipart upload(s) older than 7 days under %s "
                    "(re-run with --gc-abandoned-uploads to delete)",
                    len(old),
                    data_dir,
                )
    except Exception:
        log.exception("failed to scan for abandoned multipart uploads (non-fatal)")

    uvicorn.run(
        "nanio.app:build_app",
        factory=True,
        host=settings.host,
        port=settings.port,
        workers=settings.workers,
        access_log=settings.access_log,
        log_level=settings.log_level,
    )
    return 0


def _path_or_none(value: str | None) -> Path | None:
    return Path(value) if value else None


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.func is None:
        parser.print_help()
        return 0
    rc = args.func(args)
    return int(rc or 0)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
