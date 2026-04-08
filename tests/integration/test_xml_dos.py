"""Regression tests for the XML DoS hardening (security audit H2/H3/H4).

These exercise:

- **H2 — billion-laughs**: nested-entity XML bodies must be rejected by
  defusedxml before any expansion happens.
- **H3 — unbounded body**: oversized request bodies (>1 MiB by default)
  must be rejected with `EntityTooLarge` before parsing.
- **H4 — batch size caps**: a `DeleteObjects` body with >1000 keys or a
  `CompleteMultipartUpload` body with >10 000 parts must be rejected.

All tests use the in-process httpx ASGI client so we can craft pathological
bodies without going through boto3's well-behaved client.
"""

from __future__ import annotations

from xml.etree import ElementTree as ET

import pytest

# ----------------------------------------------------------------------
# H2: billion-laughs / entity expansion
# ----------------------------------------------------------------------


BILLION_LAUGHS = b"""<?xml version="1.0"?>
<!DOCTYPE lolz [
  <!ENTITY lol "lol">
  <!ENTITY lol2 "&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;">
  <!ENTITY lol3 "&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;">
  <!ENTITY lol4 "&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;">
]>
<Delete><Object><Key>&lol4;</Key></Object></Delete>
"""


@pytest.fixture(autouse=True)
async def _bucket(asgi_client):
    await asgi_client.put("/widgets")


@pytest.mark.asyncio
async def test_billion_laughs_rejected_on_delete_objects(asgi_client):
    r = await asgi_client.post(
        "/widgets?delete",
        content=BILLION_LAUGHS,
        headers={"content-type": "application/xml"},
    )
    assert r.status_code == 400
    root = ET.fromstring(r.content)
    assert root.find("Code").text == "MalformedXML"


@pytest.mark.asyncio
async def test_billion_laughs_rejected_on_complete_multipart(asgi_client):
    r = await asgi_client.post(
        "/widgets/some-key?uploadId=fake-upload-id",
        content=BILLION_LAUGHS,
        headers={"content-type": "application/xml"},
    )
    assert r.status_code == 400
    root = ET.fromstring(r.content)
    assert root.find("Code").text == "MalformedXML"


@pytest.mark.asyncio
async def test_external_entity_rejected(asgi_client):
    """External-entity (XXE) bodies must also be rejected."""
    body = b"""<?xml version="1.0"?>
<!DOCTYPE root [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>
<Delete><Object><Key>&xxe;</Key></Object></Delete>"""
    r = await asgi_client.post(
        "/widgets?delete",
        content=body,
        headers={"content-type": "application/xml"},
    )
    assert r.status_code == 400
    # The response body should NOT contain anything from /etc/passwd.
    assert b"root:" not in r.content


# ----------------------------------------------------------------------
# H3: unbounded body
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_oversized_delete_objects_body_rejected(asgi_client):
    # Send a 2 MiB body — well over the 1 MiB cap. Use a single huge XML
    # comment so the body is valid-looking XML if it were ever parsed.
    body = b"<Delete>" + b"<!-- " + b"x" * (2 * 1024 * 1024) + b" -->" + b"</Delete>"
    r = await asgi_client.post(
        "/widgets?delete",
        content=body,
        headers={"content-type": "application/xml"},
    )
    assert r.status_code == 400
    root = ET.fromstring(r.content)
    assert root.find("Code").text == "EntityTooLarge"


@pytest.mark.asyncio
async def test_oversized_complete_multipart_body_rejected(asgi_client):
    body = b"<CompleteMultipartUpload>" + b"x" * (2 * 1024 * 1024) + b"</CompleteMultipartUpload>"
    r = await asgi_client.post(
        "/widgets/some-key?uploadId=fake",
        content=body,
        headers={"content-type": "application/xml"},
    )
    assert r.status_code == 400
    root = ET.fromstring(r.content)
    assert root.find("Code").text == "EntityTooLarge"


# ----------------------------------------------------------------------
# H4: batch size caps
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_objects_batch_size_capped(asgi_client):
    # Construct a DeleteObjects body with 1001 keys (one over the cap).
    keys_xml = b"".join(f"<Object><Key>k-{i}</Key></Object>".encode() for i in range(1001))
    body = b"<Delete>" + keys_xml + b"</Delete>"
    # Make sure we're under the body-size cap so we hit the batch cap, not
    # the body cap. 1001 * ~25 bytes = ~25 KiB, well under 1 MiB.
    assert len(body) < 1024 * 1024
    r = await asgi_client.post(
        "/widgets?delete",
        content=body,
        headers={"content-type": "application/xml"},
    )
    assert r.status_code == 400
    root = ET.fromstring(r.content)
    assert root.find("Code").text == "MalformedXML"
    assert b"1000" in r.content  # error message mentions the cap


@pytest.mark.asyncio
async def test_delete_objects_at_batch_cap_succeeds(asgi_client):
    # Exactly 1000 keys must succeed.
    keys_xml = b"".join(f"<Object><Key>k-{i}</Key></Object>".encode() for i in range(1000))
    body = b"<Delete>" + keys_xml + b"</Delete>"
    r = await asgi_client.post(
        "/widgets?delete",
        content=body,
        headers={"content-type": "application/xml"},
    )
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_complete_multipart_batch_size_capped(asgi_client):
    # 10001 parts in the body, all referencing nonexistent parts. The
    # cap should fire at parse time, before storage is consulted.
    parts_xml = b"".join(
        f'<Part><PartNumber>{i}</PartNumber><ETag>"abc"</ETag></Part>'.encode()
        for i in range(1, 10002)
    )
    body = b"<CompleteMultipartUpload>" + parts_xml + b"</CompleteMultipartUpload>"
    # Body must fit within the body cap so we hit the batch cap first.
    assert len(body) < 1024 * 1024
    r = await asgi_client.post(
        "/widgets/some-key?uploadId=fake",
        content=body,
        headers={"content-type": "application/xml"},
    )
    assert r.status_code == 400
    root = ET.fromstring(r.content)
    assert root.find("Code").text == "MalformedXML"
    assert b"10000" in r.content


# ----------------------------------------------------------------------
# Sanity: well-formed bodies still work
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_normal_delete_objects_still_works(asgi_client):
    await asgi_client.put("/widgets/foo", content=b"x")
    await asgi_client.put("/widgets/bar", content=b"y")
    body = b"<Delete><Object><Key>foo</Key></Object><Object><Key>bar</Key></Object></Delete>"
    r = await asgi_client.post(
        "/widgets?delete",
        content=body,
        headers={"content-type": "application/xml"},
    )
    assert r.status_code == 200
    assert b"<Deleted>" in r.content
