"""Integration tests for ListObjectsV2 — prefix, delimiter, pagination, encoding."""

from __future__ import annotations

from urllib.parse import unquote
from xml.etree import ElementTree as ET

import pytest

from nanio.xml import S3_NS


def _ns(tag: str) -> str:
    return f"{{{S3_NS}}}{tag}"


def _parse_list(content: bytes):
    root = ET.fromstring(content)
    contents = [
        (e.find(_ns("Key")).text, int(e.find(_ns("Size")).text))
        for e in root.findall(_ns("Contents"))
    ]
    common = [e.find(_ns("Prefix")).text for e in root.findall(_ns("CommonPrefixes"))]
    is_truncated = root.find(_ns("IsTruncated")).text == "true"
    next_token_el = root.find(_ns("NextContinuationToken"))
    next_token = next_token_el.text if next_token_el is not None else None
    return contents, common, is_truncated, next_token


@pytest.fixture(autouse=True)
async def _bucket(asgi_client):
    await asgi_client.put("/widgets")


@pytest.mark.asyncio
async def test_list_empty(asgi_client):
    r = await asgi_client.get("/widgets")
    assert r.status_code == 200
    contents, common, truncated, _ = _parse_list(r.content)
    assert contents == []
    assert common == []
    assert truncated is False


@pytest.mark.asyncio
async def test_list_basic(asgi_client):
    for k in ["alpha.txt", "beta.txt", "gamma.txt"]:
        await asgi_client.put(f"/widgets/{k}", content=b"x")
    r = await asgi_client.get("/widgets")
    contents, _, _, _ = _parse_list(r.content)
    keys = [k for k, _ in contents]
    assert keys == ["alpha.txt", "beta.txt", "gamma.txt"]


@pytest.mark.asyncio
async def test_list_with_prefix(asgi_client):
    for k in ["logs/jan.txt", "logs/feb.txt", "data/raw.bin"]:
        await asgi_client.put(f"/widgets/{k}", content=b"x")
    r = await asgi_client.get("/widgets?prefix=logs/")
    contents, _, _, _ = _parse_list(r.content)
    keys = sorted(k for k, _ in contents)
    assert keys == ["logs/feb.txt", "logs/jan.txt"]


@pytest.mark.asyncio
async def test_list_with_delimiter(asgi_client):
    for k in ["logs/jan.txt", "logs/feb.txt", "data/raw.bin", "top.txt"]:
        await asgi_client.put(f"/widgets/{k}", content=b"x")
    r = await asgi_client.get("/widgets?delimiter=/")
    contents, common, _, _ = _parse_list(r.content)
    assert [k for k, _ in contents] == ["top.txt"]
    assert sorted(common) == ["data/", "logs/"]


@pytest.mark.asyncio
async def test_pagination_round_trip(asgi_client):
    for i in range(7):
        await asgi_client.put(f"/widgets/k-{i}.txt", content=b"x")
    r1 = await asgi_client.get("/widgets?max-keys=3")
    c1, _, t1, tok1 = _parse_list(r1.content)
    assert len(c1) == 3
    assert t1 is True
    assert tok1

    r2 = await asgi_client.get(f"/widgets?max-keys=3&continuation-token={tok1}")
    c2, _, t2, tok2 = _parse_list(r2.content)
    assert len(c2) == 3
    assert t2 is True

    r3 = await asgi_client.get(f"/widgets?max-keys=3&continuation-token={tok2}")
    c3, _, t3, _ = _parse_list(r3.content)
    assert len(c3) == 1
    assert t3 is False

    all_keys = [k for k, _ in c1 + c2 + c3]
    assert all_keys == [f"k-{i}.txt" for i in range(7)]


@pytest.mark.asyncio
async def test_encoding_type_url(asgi_client):
    # PUT a key with a literal space, sent through httpx so it gets percent-
    # encoded on the wire. The server stores the decoded form ("space key.txt").
    await asgi_client.put("/widgets/space%20key.txt", content=b"x")
    r = await asgi_client.get("/widgets?encoding-type=url")
    assert r.status_code == 200
    contents, _, _, _ = _parse_list(r.content)
    keys_in_response = [k for k, _ in contents]
    # In url-encoded mode the response key should contain "%20", not a literal space.
    assert "space%20key.txt" in keys_in_response
    # And after decoding we recover the on-disk key.
    assert "space key.txt" in [unquote(k) for k in keys_in_response]


@pytest.mark.asyncio
async def test_encoding_type_off_returns_raw_key(asgi_client):
    await asgi_client.put("/widgets/space%20key.txt", content=b"x")
    r = await asgi_client.get("/widgets")
    contents, _, _, _ = _parse_list(r.content)
    assert ("space key.txt", 1) in contents


@pytest.mark.asyncio
async def test_max_keys_capped(asgi_client):
    for i in range(5):
        await asgi_client.put(f"/widgets/k{i}", content=b"x")
    r = await asgi_client.get("/widgets?max-keys=99999")
    contents, _, truncated, _ = _parse_list(r.content)
    assert len(contents) == 5
    assert truncated is False


@pytest.mark.asyncio
async def test_pagination_across_many_files(asgi_client):
    """Sanity-check that pagination works for hundreds of keys."""
    n = 250
    for i in range(n):
        await asgi_client.put(f"/widgets/k-{i:04d}.txt", content=b"x")
    seen: list[str] = []
    token: str | None = None
    while True:
        url = "/widgets?max-keys=50"
        if token:
            url += f"&continuation-token={token}"
        r = await asgi_client.get(url)
        contents, _, truncated, next_token = _parse_list(r.content)
        seen.extend(k for k, _ in contents)
        if not truncated:
            break
        token = next_token
    assert seen == [f"k-{i:04d}.txt" for i in range(n)]
