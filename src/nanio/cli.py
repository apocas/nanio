"""Command-line entry point for nanio.

`nanio serve` parses flags and env vars, builds a `Settings`, and hands
off to `uvicorn.run` with the app factory.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from nanio import __version__

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
        description="Run the nanio HTTP server. Requires NANIO_ACCESS_KEY and "
        "NANIO_SECRET_KEY env vars (unless --credentials-file is used).",
    )
    serve.add_argument(
        "--data-dir",
        type=Path,
        default=os.environ.get("NANIO_DATA_DIR"),
        help="Root directory for bucket data (default: ./nanio-data, env: NANIO_DATA_DIR)",
    )
    serve.add_argument(
        "--host",
        default=os.environ.get("NANIO_HOST", "0.0.0.0"),
        help="Bind host (default: 0.0.0.0, env: NANIO_HOST)",
    )
    serve.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("NANIO_PORT", "9000")),
        help="Bind port (default: 9000, env: NANIO_PORT)",
    )
    serve.add_argument(
        "--workers",
        type=int,
        default=int(os.environ.get("NANIO_WORKERS", "1")),
        help="Number of uvicorn worker processes (default: 1, env: NANIO_WORKERS)",
    )
    serve.add_argument(
        "--region",
        default=os.environ.get("NANIO_REGION", "us-east-1"),
        help="S3 region to report (default: us-east-1, env: NANIO_REGION)",
    )
    serve.add_argument(
        "--credentials-file",
        type=Path,
        default=os.environ.get("NANIO_CREDENTIALS_FILE"),
        help="TOML file with multi-user credentials (env: NANIO_CREDENTIALS_FILE)",
    )
    serve.add_argument(
        "--log-level",
        choices=["debug", "info", "warning", "error"],
        default=os.environ.get("NANIO_LOG_LEVEL", "info"),
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


def _cmd_serve(args: argparse.Namespace) -> int:
    import uvicorn

    from nanio.auth.credentials import (
        EnvCredentialResolver,
        TomlFileCredentialResolver,
    )
    from nanio.config import DEFAULT_DATA_DIR, Settings
    from nanio.logging import setup_logging

    setup_logging(args.log_level)

    if args.credentials_file:
        try:
            credentials = TomlFileCredentialResolver(args.credentials_file)
        except (FileNotFoundError, ValueError) as exc:
            print(f"nanio: failed to load credentials file: {exc}", file=sys.stderr)
            return 2
    else:
        try:
            credentials = EnvCredentialResolver()
        except ValueError as exc:
            print(f"nanio: {exc}", file=sys.stderr)
            return 2

    data_dir = (args.data_dir or DEFAULT_DATA_DIR).resolve()

    settings = Settings(
        data_dir=data_dir,
        host=args.host,
        port=args.port,
        workers=args.workers,
        region=args.region,
        credentials=credentials,
        log_level=args.log_level,
        access_log=args.access_log,
    )

    # Persist settings into env vars so worker processes (which re-import the
    # app via the uvicorn factory) construct an identical Settings instance.
    os.environ["NANIO_DATA_DIR"] = str(data_dir)
    os.environ["NANIO_REGION"] = args.region
    if args.credentials_file:
        os.environ["NANIO_CREDENTIALS_FILE"] = str(args.credentials_file)

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
