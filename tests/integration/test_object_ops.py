"""Integration tests for object PUT/GET/HEAD/DELETE via the ASGI app."""

from __future__ import annotations

import base64
import hashlib

import pytest


@pytest.fixture(autouse=True)
async def _bucket(asgi_client):
    await asgi_client.put("/widgets")


@pytest.mark.asyncio
async def test_put_then_get_round_trip(asgi_client):
    payload = b"hello world"
    r = await asgi_client.put("/widgets/hello.txt", content=payload)
    assert r.status_code == 200
    expected_etag = f'"{hashlib.md5(payload, usedforsecurity=False).hexdigest()}"'
    assert r.headers["etag"] == expected_etag

    r = await asgi_client.get("/widgets/hello.txt")
    assert r.status_code == 200
    assert r.content == payload
    assert r.headers["etag"] == expected_etag
    assert r.headers["accept-ranges"] == "bytes"
    assert r.headers["content-length"] == str(len(payload))


@pytest.mark.asyncio
async def test_head_returns_metadata_no_body(asgi_client):
    await asgi_client.put("/widgets/hello.txt", content=b"abc", headers={"content-type": "text/plain"})
    r = await asgi_client.head("/widgets/hello.txt")
    assert r.status_code == 200
    assert r.headers["content-length"] == "3"
    assert r.headers["content-type"] == "text/plain"


@pytest.mark.asyncio
async def test_user_metadata_round_trip(asgi_client):
    headers = {
        "x-amz-meta-author": "alice",
        "x-amz-meta-purpose": "test",
        "content-type": "application/json",
    }
    await asgi_client.put("/widgets/k.json", content=b"{}", headers=headers)
    r = await asgi_client.head("/widgets/k.json")
    assert r.headers["x-amz-meta-author"] == "alice"
    assert r.headers["x-amz-meta-purpose"] == "test"
    assert r.headers["content-type"] == "application/json"


@pytest.mark.asyncio
async def test_get_missing_object(asgi_client):
    r = await asgi_client.get("/widgets/nope")
    assert r.status_code == 404
    assert b"NoSuchKey" in r.content


@pytest.mark.asyncio
async def test_get_missing_bucket(asgi_client):
    r = await asgi_client.get("/ghost/foo")
    assert r.status_code == 404
    assert b"NoSuchBucket" in r.content


@pytest.mark.asyncio
async def test_delete_object(asgi_client):
    await asgi_client.put("/widgets/foo", content=b"x")
    r = await asgi_client.delete("/widgets/foo")
    assert r.status_code == 204
    r = await asgi_client.get("/widgets/foo")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_delete_object_idempotent(asgi_client):
    r = await asgi_client.delete("/widgets/never-existed")
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_put_with_content_md5_ok(asgi_client):
    payload = b"hello"
    md5_b64 = base64.b64encode(hashlib.md5(payload, usedforsecurity=False).digest()).decode()
    r = await asgi_client.put("/widgets/k", content=payload, headers={"content-md5": md5_b64})
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_put_with_bad_content_md5(asgi_client):
    bad = base64.b64encode(b"\x00" * 16).decode()
    r = await asgi_client.put("/widgets/k", content=b"hello", headers={"content-md5": bad})
    assert r.status_code == 400
    assert b"BadDigest" in r.content


@pytest.mark.asyncio
async def test_get_range(asgi_client):
    await asgi_client.put("/widgets/k", content=b"abcdefghij")
    r = await asgi_client.get("/widgets/k", headers={"range": "bytes=2-5"})
    assert r.status_code == 206
    assert r.content == b"cdef"
    assert r.headers["content-range"] == "bytes 2-5/10"
    assert r.headers["content-length"] == "4"


@pytest.mark.asyncio
async def test_get_range_open_ended(asgi_client):
    await asgi_client.put("/widgets/k", content=b"abcdefghij")
    r = await asgi_client.get("/widgets/k", headers={"range": "bytes=5-"})
    assert r.status_code == 206
    assert r.content == b"fghij"


@pytest.mark.asyncio
async def test_put_object_with_subkey(asgi_client):
    await asgi_client.put("/widgets/path/to/deep/file.txt", content=b"deep")
    r = await asgi_client.get("/widgets/path/to/deep/file.txt")
    assert r.status_code == 200
    assert r.content == b"deep"
