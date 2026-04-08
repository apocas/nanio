"""Integration tests for HTTP error responses.

Verifies that every S3Error subclass we raise from a handler is serialized
to a well-formed XML body with the correct HTTP status code.
"""

from __future__ import annotations

from xml.etree import ElementTree as ET

import pytest


def _parse_error(content: bytes) -> dict[str, str]:
    root = ET.fromstring(content)
    return {child.tag: (child.text or "") for child in root}


@pytest.mark.asyncio
async def test_no_such_bucket_on_put_object(asgi_client):
    r = await asgi_client.put("/ghost/foo", content=b"x")
    assert r.status_code == 404
    fields = _parse_error(r.content)
    assert fields["Code"] == "NoSuchBucket"


@pytest.mark.asyncio
async def test_no_such_bucket_on_get(asgi_client):
    r = await asgi_client.get("/ghost/foo")
    assert r.status_code == 404
    assert _parse_error(r.content)["Code"] == "NoSuchBucket"


@pytest.mark.asyncio
async def test_no_such_key(asgi_client):
    await asgi_client.put("/widgets")
    r = await asgi_client.get("/widgets/missing")
    assert r.status_code == 404
    assert _parse_error(r.content)["Code"] == "NoSuchKey"


@pytest.mark.asyncio
async def test_invalid_bucket_name(asgi_client):
    r = await asgi_client.put("/INVALID_NAME")
    assert r.status_code == 400
    assert _parse_error(r.content)["Code"] == "InvalidBucketName"


@pytest.mark.asyncio
async def test_bucket_already_exists(asgi_client):
    await asgi_client.put("/widgets")
    r = await asgi_client.put("/widgets")
    assert r.status_code == 409
    assert _parse_error(r.content)["Code"] == "BucketAlreadyOwnedByYou"


@pytest.mark.asyncio
async def test_bucket_not_empty(asgi_client):
    await asgi_client.put("/widgets")
    await asgi_client.put("/widgets/foo", content=b"x")
    r = await asgi_client.delete("/widgets")
    assert r.status_code == 409
    assert _parse_error(r.content)["Code"] == "BucketNotEmpty"


@pytest.mark.asyncio
async def test_bad_digest(asgi_client):
    import base64

    await asgi_client.put("/widgets")
    bad = base64.b64encode(b"\x00" * 16).decode()
    r = await asgi_client.put("/widgets/k", content=b"hello", headers={"content-md5": bad})
    assert r.status_code == 400
    assert _parse_error(r.content)["Code"] == "BadDigest"


@pytest.mark.asyncio
async def test_invalid_argument_on_bad_range(asgi_client):
    await asgi_client.put("/widgets")
    await asgi_client.put("/widgets/k", content=b"abc")
    r = await asgi_client.get("/widgets/k", headers={"range": "bytes=10-20"})
    assert r.status_code == 400
    assert _parse_error(r.content)["Code"] == "InvalidArgument"


@pytest.mark.asyncio
async def test_error_response_xml_well_formed(asgi_client):
    """Every error body must be parseable XML and contain the standard fields."""
    r = await asgi_client.get("/ghost-bucket/key")
    assert r.status_code == 404
    root = ET.fromstring(r.content)
    assert root.tag == "Error"
    tags = {child.tag for child in root}
    assert "Code" in tags
    assert "Message" in tags
    assert "RequestId" in tags


@pytest.mark.asyncio
async def test_unexpected_exception_returns_xml_internal_error(app):
    """Security audit M3: any non-S3Error exception must be caught by the
    catch-all handler and serialized to a generic InternalError XML body —
    never leaking the Python exception class or message to the client.

    Uses a dedicated httpx client with `raise_app_exceptions=False` because
    Starlette's ServerErrorMiddleware deliberately re-raises after sending
    the 500 so test clients can see the underlying error. Real HTTP
    clients only ever see the response, never the re-raised exception.
    """
    from httpx import ASGITransport, AsyncClient
    from starlette.routing import Route

    async def boom(request):
        raise RuntimeError("nuclear codes: 12345")

    # Inject a route that always raises a non-S3Error.
    app.router.routes.insert(0, Route("/__boom__", boom))

    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://nanio.test") as client:
        r = await client.get("/__boom__")

    assert r.status_code == 500
    body = r.content.decode()
    # Generic body, no leaked exception info.
    assert "InternalError" in body
    assert "RuntimeError" not in body
    assert "nuclear codes" not in body
    assert "12345" not in body
    # And it must be valid XML in the standard format.
    root = ET.fromstring(r.content)
    assert root.tag == "Error"
    assert root.find("Code").text == "InternalError"
