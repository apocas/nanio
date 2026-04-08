"""Service-level handlers (operations on `/` with no bucket)."""

from __future__ import annotations

from starlette.requests import Request
from starlette.responses import Response

from nanio.app_state import get_storage
from nanio.xml import list_buckets_xml


async def list_buckets(request: Request) -> Response:
    storage = get_storage(request)
    buckets = storage.list_buckets()
    body = list_buckets_xml((b.name, b.created) for b in buckets)
    return Response(content=body, media_type="application/xml", status_code=200)
