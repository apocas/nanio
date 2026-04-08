"""Integration tests for service-level ops (`GET /` ListBuckets)."""

from __future__ import annotations

from xml.etree import ElementTree as ET

import pytest

from nanio.xml import S3_NS


def _ns(tag: str) -> str:
    return f"{{{S3_NS}}}{tag}"


@pytest.mark.asyncio
async def test_list_buckets_empty(asgi_client):
    r = await asgi_client.get("/")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/xml")
    root = ET.fromstring(r.content)
    assert root.tag == _ns("ListAllMyBucketsResult")
    buckets = root.find(_ns("Buckets"))
    assert buckets is not None
    assert len(list(buckets)) == 0


@pytest.mark.asyncio
async def test_list_buckets_after_create(asgi_client):
    await asgi_client.put("/widgets")
    await asgi_client.put("/sprockets")
    r = await asgi_client.get("/")
    assert r.status_code == 200
    root = ET.fromstring(r.content)
    names = sorted(e.text for e in root.iter(_ns("Name")))
    assert names == ["sprockets", "widgets"]
