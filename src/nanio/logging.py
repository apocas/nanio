"""Structured logging setup for nanio.

Plain text by default. Keeps it simple — no JSON, no third-party logger.
The format includes a timestamp, level, logger name, and message.
"""

from __future__ import annotations

import logging
import sys

_LOG_FORMAT = "%(asctime)s %(levelname)-7s %(name)s: %(message)s"
_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"


def setup_logging(level: str = "info") -> None:
    """Configure the root logger. Idempotent."""
    numeric = getattr(logging, level.upper(), None)
    if not isinstance(numeric, int):
        raise ValueError(f"unknown log level: {level!r}")

    root = logging.getLogger()
    root.setLevel(numeric)

    # Remove any pre-existing handlers we own to keep this idempotent.
    for handler in list(root.handlers):
        if getattr(handler, "_nanio", False):
            root.removeHandler(handler)

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT))
    handler._nanio = True  # type: ignore[attr-defined]
    root.addHandler(handler)
