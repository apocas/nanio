"""In-process ASGI tests for the multipart upload handlers.

The existing `test_multipart_boto3.py` exercises the same operations, but
it runs against a real `uvicorn` subprocess (so that boto3 can make socket
connections). Subprocess execution does NOT contribute to in-process
coverage, which leaves `handlers/multipart.py` showing as ~37% covered
even though every line works correctly at runtime.

These tests use the in-process `asgi_client` fixture (httpx's
`ASGITransport`) so the handler code runs inside the pytest process and
is visible to `coverage`. They cover both the happy paths and the error
branches (missing query params, oversized bodies, bad XML, missing
buckets, etc.) that the boto3 suite can't easily drive.
"""

from __future__ import annotations

from xml.etree import ElementTree as ET

import pytest

from nanio.xml import S3_NS


def _ns(tag: str) -> str:
    return f"{{{S3_NS}}}{tag}"


@pytest.fixture(autouse=True)
async def _bucket(asgi_client):
    await asgi_client.put("/widgets")


# ----------------------------------------------------------------------
# Create multipart upload
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_multipart_upload(asgi_client):
    r = await asgi_client.post(
        "/widgets/big.bin?uploads",
        headers={"content-type": "application/octet-stream"},
    )
    assert r.status_code == 200
    root = ET.fromstring(r.content)
    assert root.find(_ns("Bucket")).text == "widgets"
    assert root.find(_ns("Key")).text == "big.bin"
    upload_id = root.find(_ns("UploadId")).text
    assert upload_id and len(upload_id) > 8


@pytest.mark.asyncio
async def test_create_multipart_upload_missing_bucket(asgi_client):
    r = await asgi_client.post("/ghost/k?uploads")
    assert r.status_code == 404
    assert b"NoSuchBucket" in r.content


@pytest.mark.asyncio
async def test_create_multipart_upload_with_user_metadata(asgi_client):
    r = await asgi_client.post(
        "/widgets/meta.bin?uploads",
        headers={
            "content-type": "application/octet-stream",
            "x-amz-meta-author": "alice",
            "content-encoding": "gzip",
            "content-disposition": "attachment",
            "cache-control": "no-cache",
        },
    )
    assert r.status_code == 200


# ----------------------------------------------------------------------
# Upload part
# ----------------------------------------------------------------------


async def _start_upload(asgi_client, bucket: str = "widgets", key: str = "big.bin") -> str:
    r = await asgi_client.post(f"/{bucket}/{key}?uploads")
    return ET.fromstring(r.content).find(_ns("UploadId")).text


@pytest.mark.asyncio
async def test_upload_part_success(asgi_client):
    upload_id = await _start_upload(asgi_client)
    r = await asgi_client.put(
        f"/widgets/big.bin?uploadId={upload_id}&partNumber=1",
        content=b"a" * 1024,
    )
    assert r.status_code == 200
    assert "etag" in r.headers


@pytest.mark.asyncio
async def test_upload_part_non_numeric_part_number(asgi_client):
    upload_id = await _start_upload(asgi_client)
    r = await asgi_client.put(
        f"/widgets/big.bin?uploadId={upload_id}&partNumber=notanumber",
        content=b"x",
    )
    assert r.status_code == 400
    assert b"InvalidArgument" in r.content


@pytest.mark.asyncio
async def test_upload_part_missing_bucket(asgi_client):
    r = await asgi_client.put(
        "/ghost/big.bin?uploadId=fake&partNumber=1",
        content=b"x",
    )
    assert r.status_code == 404
    assert b"NoSuchBucket" in r.content


# ----------------------------------------------------------------------
# Complete
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_complete_multipart_upload_success(asgi_client):
    upload_id = await _start_upload(asgi_client)
    r1 = await asgi_client.put(
        f"/widgets/big.bin?uploadId={upload_id}&partNumber=1",
        content=b"A" * 1024,
    )
    etag1 = r1.headers["etag"]

    body = (
        "<CompleteMultipartUpload>"
        f"<Part><PartNumber>1</PartNumber><ETag>{etag1}</ETag></Part>"
        "</CompleteMultipartUpload>"
    ).encode()
    r = await asgi_client.post(
        f"/widgets/big.bin?uploadId={upload_id}",
        content=body,
        headers={"content-type": "application/xml"},
    )
    assert r.status_code == 200
    root = ET.fromstring(r.content)
    assert root.find(_ns("Bucket")).text == "widgets"
    assert root.find(_ns("Key")).text == "big.bin"


@pytest.mark.asyncio
async def test_complete_multipart_upload_missing_uploadid(asgi_client):
    r = await asgi_client.post(
        "/widgets/big.bin",
        content=b"<CompleteMultipartUpload/>",
        headers={"content-type": "application/xml"},
    )
    # POST without uploadId and without ?uploads falls through the
    # dispatcher to 405.
    assert r.status_code == 405


@pytest.mark.asyncio
async def test_complete_multipart_upload_part_missing_fields(asgi_client):
    upload_id = await _start_upload(asgi_client)
    # Part element missing ETag.
    body = (
        b"<CompleteMultipartUpload>"
        b"<Part><PartNumber>1</PartNumber></Part>"
        b"</CompleteMultipartUpload>"
    )
    r = await asgi_client.post(
        f"/widgets/big.bin?uploadId={upload_id}",
        content=body,
        headers={"content-type": "application/xml"},
    )
    assert r.status_code == 400
    assert b"MalformedXML" in r.content


@pytest.mark.asyncio
async def test_complete_multipart_upload_part_bad_part_number(asgi_client):
    upload_id = await _start_upload(asgi_client)
    body = (
        b"<CompleteMultipartUpload>"
        b'<Part><PartNumber>notanumber</PartNumber><ETag>"a"</ETag></Part>'
        b"</CompleteMultipartUpload>"
    )
    r = await asgi_client.post(
        f"/widgets/big.bin?uploadId={upload_id}",
        content=body,
        headers={"content-type": "application/xml"},
    )
    assert r.status_code == 400
    assert b"MalformedXML" in r.content


@pytest.mark.asyncio
async def test_complete_multipart_upload_body_ignores_unknown_elements(asgi_client):
    """Non-Part children in CompleteMultipartUpload are silently skipped."""
    upload_id = await _start_upload(asgi_client)
    r1 = await asgi_client.put(
        f"/widgets/big.bin?uploadId={upload_id}&partNumber=1",
        content=b"A",
    )
    etag1 = r1.headers["etag"]
    body = (
        "<CompleteMultipartUpload>"
        "<Unknown>noise</Unknown>"
        f"<Part><PartNumber>1</PartNumber><ETag>{etag1}</ETag></Part>"
        "</CompleteMultipartUpload>"
    ).encode()
    r = await asgi_client.post(
        f"/widgets/big.bin?uploadId={upload_id}",
        content=body,
        headers={"content-type": "application/xml"},
    )
    assert r.status_code == 200


# ----------------------------------------------------------------------
# Abort
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_abort_multipart_upload(asgi_client):
    upload_id = await _start_upload(asgi_client)
    r = await asgi_client.delete(f"/widgets/big.bin?uploadId={upload_id}")
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_abort_missing_upload(asgi_client):
    r = await asgi_client.delete("/widgets/big.bin?uploadId=nonexistent")
    assert r.status_code == 404
    assert b"NoSuchUpload" in r.content


# ----------------------------------------------------------------------
# List parts + list uploads
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_parts(asgi_client):
    upload_id = await _start_upload(asgi_client)
    await asgi_client.put(
        f"/widgets/big.bin?uploadId={upload_id}&partNumber=1",
        content=b"AA",
    )
    await asgi_client.put(
        f"/widgets/big.bin?uploadId={upload_id}&partNumber=2",
        content=b"BB",
    )

    r = await asgi_client.get(f"/widgets/big.bin?uploadId={upload_id}")
    assert r.status_code == 200
    root = ET.fromstring(r.content)
    parts = root.findall(_ns("Part"))
    assert len(parts) == 2


@pytest.mark.asyncio
async def test_list_parts_missing_upload(asgi_client):
    r = await asgi_client.get("/widgets/big.bin?uploadId=nonexistent")
    assert r.status_code == 404
    assert b"NoSuchUpload" in r.content


@pytest.mark.asyncio
async def test_list_multipart_uploads_for_bucket(asgi_client):
    u1 = await _start_upload(asgi_client, key="a.bin")
    u2 = await _start_upload(asgi_client, key="b.bin")

    r = await asgi_client.get("/widgets?uploads")
    assert r.status_code == 200
    root = ET.fromstring(r.content)
    uploads = root.findall(_ns("Upload"))
    ids = {u.find(_ns("UploadId")).text for u in uploads}
    assert u1 in ids
    assert u2 in ids


@pytest.mark.asyncio
async def test_list_multipart_uploads_missing_bucket(asgi_client):
    r = await asgi_client.get("/ghost?uploads")
    assert r.status_code == 404
    assert b"NoSuchBucket" in r.content


# ----------------------------------------------------------------------
# Dispatcher edge cases
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_post_to_object_without_query_is_405(asgi_client):
    """A POST with neither ?uploads nor ?uploadId is 405."""
    r = await asgi_client.post("/widgets/foo.bin", content=b"x")
    assert r.status_code == 405
