"""ASGI application factory.

Importing this module is cheap; constructing the app via `build_app(settings)`
binds storage, middleware, and routes to the given settings instance. The
factory function is what `uvicorn` calls with `factory=True`.
"""

from __future__ import annotations

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
from nanio.errors import S3Error
from nanio.handlers import build_routes
from nanio.storage.filesystem import FilesystemStorage


def _settings_from_env() -> Settings:
    data_dir = Path(os.environ.get("NANIO_DATA_DIR") or DEFAULT_DATA_DIR).resolve()
    region = os.environ.get("NANIO_REGION", "us-east-1")
    cred_file = os.environ.get("NANIO_CREDENTIALS_FILE")
    credentials = TomlFileCredentialResolver(cred_file) if cred_file else EnvCredentialResolver()
    return Settings(data_dir=data_dir, region=region, credentials=credentials)


async def _s3_error_handler(request: Request, exc: Exception) -> Response:
    assert isinstance(exc, S3Error)
    return Response(
        content=exc.to_xml(),
        status_code=exc.http_status,
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
        exception_handlers={S3Error: _s3_error_handler},
    )
    app.state.settings = settings
    app.state.storage = storage
    return app
