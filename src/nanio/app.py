"""ASGI application factory.

Importing this module is cheap; constructing the app via `build_app(settings)`
binds storage, middleware, and routes to the given settings instance. The
factory function is what `uvicorn` calls with `factory=True`.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import Response

from nanio.auth.credentials import (
    EnvCredentialResolver,
    TomlFileCredentialResolver,
)
from nanio.auth.middleware import SigV4Middleware
from nanio.config import DEFAULT_DATA_DIR, Settings
from nanio.errors import InternalError, S3Error
from nanio.handlers import build_routes
from nanio.storage.filesystem import FilesystemStorage

_log = logging.getLogger("nanio.app")


def _settings_from_env() -> Settings:
    data_dir = Path(os.environ.get("NANIO_DATA_DIR") or DEFAULT_DATA_DIR).resolve()
    region = os.environ.get("NANIO_REGION", "us-east-1")
    options_file = os.environ.get("NANIO_OPTIONS_FILE")
    credentials = (
        TomlFileCredentialResolver(options_file) if options_file else EnvCredentialResolver()
    )
    return Settings(data_dir=data_dir, region=region, credentials=credentials)


async def _s3_error_handler(request: Request, exc: Exception) -> Response:
    assert isinstance(exc, S3Error)
    return Response(
        content=exc.to_xml(),
        status_code=exc.http_status,
        media_type="application/xml",
    )


async def _unexpected_error_handler(request: Request, exc: Exception) -> Response:
    """Catch-all for any non-S3Error exception (security audit finding M3).

    Without this, Starlette's default handler returns a plain-text 500
    that may leak the exception class name and message — breaking the
    wire contract (clients expect S3 XML errors) and disclosing internal
    state. We log the full traceback server-side and return a generic
    `InternalError` XML body.
    """
    _log.exception("unhandled exception in request handler", exc_info=exc)
    err = InternalError()
    return Response(
        content=err.to_xml(),
        status_code=err.http_status,
        media_type="application/xml",
    )


def build_app(settings: Settings | None = None) -> Starlette:
    if settings is None:
        # When uvicorn imports us via the factory, we reconstruct settings
        # from the environment so that worker processes match the parent.
        settings = _settings_from_env()

    storage = FilesystemStorage(settings.data_dir, chunk_size=settings.chunk_size)

    middleware = [
        Middleware(
            SigV4Middleware,
            credentials=settings.credentials,
            disabled=settings.auth_disabled,
        ),
    ]

    app = Starlette(
        debug=False,
        routes=build_routes(),
        middleware=middleware,
        exception_handlers={
            S3Error: _s3_error_handler,
            Exception: _unexpected_error_handler,
        },
    )
    app.state.settings = settings
    app.state.storage = storage
    return app
