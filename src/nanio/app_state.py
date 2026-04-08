"""Helpers for fetching app-wide state from the request scope.

Starlette stores anything we attach to `app.state` and makes it reachable
via `request.app.state`. We expose typed accessors here so handlers don't
spell the attribute names directly.
"""

from __future__ import annotations

from starlette.requests import Request

from nanio.config import Settings
from nanio.storage.backend import Storage


def get_storage(request: Request) -> Storage:
    return request.app.state.storage  # type: ignore[no-any-return]


def get_settings(request: Request) -> Settings:
    return request.app.state.settings  # type: ignore[no-any-return]
