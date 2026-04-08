"""Tests for nanio.xml response builders."""

from __future__ import annotations

from datetime import UTC, datetime
from xml.etree import ElementTree as ET

from nanio.xml import (
    S3_NS,
    complete_multipart_upload_xml,
    copy_object_result_xml,
    delete_result_xml,
    initiate_multipart_upload_xml,
    list_buckets_xml,
    list_multipart_uploads_xml,
    list_objects_v2_xml,
    list_parts_xml,
    location_constraint_xml,
)


def _parse(xml_bytes: bytes) -> ET.Element:
    return ET.fromstring(xml_bytes)


def _ns(tag: str) -> str:
    return f"{{{S3_NS}}}{tag}"


def test_list_buckets_empty():
    xml = list_buckets_xml([])
    root = _parse(xml)
    assert root.tag == _ns("ListAllMyBucketsResult")
    buckets = root.find(_ns("Buckets"))
    assert buckets is not None
    assert len(list(buckets)) == 0


def test_list_buckets_multiple():
    when = datetime(2026, 4, 8, 12, 0, 0, tzinfo=UTC)
    xml = list_buckets_xml([("alpha", when), ("beta", when)])
    root = _parse(xml)
    names = [e.text for e in root.iter(_ns("Name"))]
    assert names == ["alpha", "beta"]
    creations = [e.text for e in root.iter(_ns("CreationDate"))]
    assert all(c == "2026-04-08T12:00:00.000Z" for c in creations)


def test_list_buckets_escapes_special_chars():
    when = datetime(2026, 4, 8, tzinfo=UTC)
    xml = list_buckets_xml([("a&b", when)])
    assert b"a&amp;b" in xml


def test_list_objects_v2_basic():
    when = datetime(2026, 1, 1, tzinfo=UTC)
    xml = list_objects_v2_xml(
        bucket="widgets",
        contents=[("foo.txt", when, '"deadbeef"', 100)],
        common_prefixes=[],
        prefix="",
        max_keys=1000,
    )
    root = _parse(xml)
    assert root.tag == _ns("ListBucketResult")
    assert root.find(_ns("Name")).text == "widgets"
    assert root.find(_ns("KeyCount")).text == "1"
    assert root.find(_ns("IsTruncated")).text == "false"
    contents = root.findall(_ns("Contents"))
    assert len(contents) == 1
    assert contents[0].find(_ns("Key")).text == "foo.txt"
    assert contents[0].find(_ns("Size)".replace(")", ""))).text == "100"
    assert contents[0].find(_ns("StorageClass")).text == "STANDARD"


def test_list_objects_v2_truncated_with_token():
    xml = list_objects_v2_xml(
        bucket="widgets",
        contents=[],
        common_prefixes=[],
        prefix="p/",
        delimiter="/",
        max_keys=10,
        is_truncated=True,
        next_continuation_token="abc123",
    )
    root = _parse(xml)
    assert root.find(_ns("IsTruncated")).text == "true"
    assert root.find(_ns("NextContinuationToken")).text == "abc123"
    assert root.find(_ns("Delimiter")).text == "/"
    assert root.find(_ns("Prefix")).text == "p/"


def test_list_objects_v2_common_prefixes():
    xml = list_objects_v2_xml(
        bucket="widgets",
        contents=[],
        common_prefixes=["a/", "b/", "c/"],
        prefix="",
        delimiter="/",
    )
    root = _parse(xml)
    cps = root.findall(_ns("CommonPrefixes"))
    prefixes = [cp.find(_ns("Prefix")).text for cp in cps]
    assert prefixes == ["a/", "b/", "c/"]


def test_list_objects_v2_url_encoding():
    when = datetime(2026, 1, 1, tzinfo=UTC)
    xml = list_objects_v2_xml(
        bucket="widgets",
        contents=[("a b/c?d.txt", when, '"e"', 1)],
        common_prefixes=[],
        encoding_type="url",
    )
    root = _parse(xml)
    assert root.find(_ns("EncodingType")).text == "url"
    key = root.find(_ns("Contents")).find(_ns("Key")).text
    assert key == "a%20b%2Fc%3Fd.txt"


def test_initiate_multipart_upload_xml():
    xml = initiate_multipart_upload_xml(bucket="b", key="k", upload_id="u-123")
    root = _parse(xml)
    assert root.find(_ns("Bucket")).text == "b"
    assert root.find(_ns("Key")).text == "k"
    assert root.find(_ns("UploadId")).text == "u-123"


def test_complete_multipart_upload_xml():
    xml = complete_multipart_upload_xml(
        bucket="b",
        key="k",
        etag='"deadbeef-3"',
        location="http://localhost:9000/b/k",
    )
    root = _parse(xml)
    assert root.find(_ns("ETag")).text == '"deadbeef-3"'
    assert root.find(_ns("Location")).text == "http://localhost:9000/b/k"


def test_list_parts_xml():
    when = datetime(2026, 1, 1, tzinfo=UTC)
    xml = list_parts_xml(
        bucket="b",
        key="k",
        upload_id="u-1",
        parts_listed=[
            (1, when, '"a"', 5_242_880),
            (2, when, '"b"', 5_242_880),
            (3, when, '"c"', 1024),
        ],
    )
    root = _parse(xml)
    parts = root.findall(_ns("Part"))
    assert len(parts) == 3
    assert [p.find(_ns("PartNumber")).text for p in parts] == ["1", "2", "3"]


def test_list_multipart_uploads_xml():
    when = datetime(2026, 1, 1, tzinfo=UTC)
    xml = list_multipart_uploads_xml(
        bucket="b",
        uploads=[("k1", "u1", when), ("k2", "u2", when)],
    )
    root = _parse(xml)
    uploads = root.findall(_ns("Upload"))
    assert len(uploads) == 2
    assert [u.find(_ns("Key")).text for u in uploads] == ["k1", "k2"]


def test_delete_result_xml():
    xml = delete_result_xml(
        deleted=["a", "b"],
        errors=[("c", "AccessDenied", "Access Denied")],
    )
    root = _parse(xml)
    deleted = root.findall(_ns("Deleted"))
    assert len(deleted) == 2
    assert [d.find(_ns("Key")).text for d in deleted] == ["a", "b"]
    errors = root.findall(_ns("Error"))
    assert len(errors) == 1
    assert errors[0].find(_ns("Key")).text == "c"
    assert errors[0].find(_ns("Code")).text == "AccessDenied"


def test_copy_object_result_xml():
    when = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    xml = copy_object_result_xml(etag='"abc"', last_modified=when)
    root = _parse(xml)
    assert root.find(_ns("ETag")).text == '"abc"'
    assert root.find(_ns("LastModified")).text == "2026-01-01T12:00:00.000Z"


def test_location_constraint_xml():
    xml = location_constraint_xml("us-east-1")
    root = _parse(xml)
    assert root.tag == _ns("LocationConstraint")
    assert root.text == "us-east-1"


def test_xml_starts_with_declaration_and_is_utf8():
    xml = list_buckets_xml([])
    assert xml.startswith(b'<?xml version="1.0" encoding="UTF-8"?>')
    # Round-trip through utf-8 must work.
    xml.decode("utf-8")
