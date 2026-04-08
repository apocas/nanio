"""Integration tests for bucket CRUD."""

from __future__ import annotations

from xml.etree import ElementTree as ET

import pytest

from nanio.xml import S3_NS


def _ns(tag: str) -> str:
    return f"{{{S3_NS}}}{tag}"


@pytest.mark.asyncio
async def test_create_bucket(asgi_client):
    r = await asgi_client.put("/widgets")
    assert r.status_code == 200
    assert r.headers.get("location") == "/widgets"


@pytest.mark.asyncio
async def test_head_bucket_existing(asgi_client):
    await asgi_client.put("/widgets")
    r = await asgi_client.head("/widgets")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_head_bucket_missing(asgi_client):
    r = await asgi_client.head("/widgets")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_delete_empty_bucket(asgi_client):
    await asgi_client.put("/widgets")
    r = await asgi_client.delete("/widgets")
    assert r.status_code == 204
    r = await asgi_client.head("/widgets")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_delete_non_empty_bucket(asgi_client):
    await asgi_client.put("/widgets")
    await asgi_client.put("/widgets/foo.txt", content=b"hi")
    r = await asgi_client.delete("/widgets")
    assert r.status_code == 409
    body = r.content.decode()
    assert "BucketNotEmpty" in body


@pytest.mark.asyncio
async def test_create_bucket_twice_raises(asgi_client):
    await asgi_client.put("/widgets")
    r = await asgi_client.put("/widgets")
    assert r.status_code == 409


@pytest.mark.asyncio
async def test_get_bucket_location(asgi_client):
    await asgi_client.put("/widgets")
    r = await asgi_client.get("/widgets?location")
    assert r.status_code == 200
    root = ET.fromstring(r.content)
    assert root.text == "us-east-1"


@pytest.mark.asyncio
async def test_invalid_bucket_name(asgi_client):
    r = await asgi_client.put("/A_B")  # uppercase + underscore
    assert r.status_code == 400
