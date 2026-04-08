"""Shared fixtures for the nanio test suite.

The fixtures here cover:

- `data_dir` — fresh tmp dir per test, becomes the storage root.
- `settings` — `Settings` constructed against `data_dir` with auth disabled
  by default. Tests that need auth enabled override this.
- `app` — built ASGI app for httpx-based tests.
- `asgi_client` — async httpx client wired to the app via ASGITransport.

Real-uvicorn-subprocess fixtures (used by boto3) live alongside their
integration tests in step 7+; the ones here are pure in-process.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from nanio.app import build_app
from nanio.auth.credentials import StaticCredentialResolver
from nanio.config import Settings


TEST_ACCESS_KEY = "test-access-key"
TEST_SECRET_KEY = "test-secret-key"


@pytest.fixture
def data_dir(tmp_path: Path) -> Path:
    d = tmp_path / "data"
    d.mkdir()
    return d


@pytest.fixture
def credentials() -> StaticCredentialResolver:
    return StaticCredentialResolver({TEST_ACCESS_KEY: TEST_SECRET_KEY})


@pytest.fixture
def settings(data_dir: Path, credentials: StaticCredentialResolver) -> Settings:
    return Settings(data_dir=data_dir, credentials=credentials, auth_disabled=True)


@pytest.fixture
def app(settings: Settings):
    return build_app(settings)


@pytest_asyncio.fixture
async def asgi_client(app) -> AsyncIterator[AsyncClient]:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://nanio.test") as client:
        yield client


@pytest.fixture(scope="session")
def event_loop_policy():
    return asyncio.DefaultEventLoopPolicy()


# Re-export the live-server fixtures so any integration test can pull them
# without an explicit import.
from tests.integration._uvicorn_fixture import (  # noqa: E402,F401
    LiveServer,
    boto3_client,
    live_server,
)
