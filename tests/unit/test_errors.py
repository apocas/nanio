"""Tests for nanio.errors S3 error hierarchy."""

from __future__ import annotations

from nanio.errors import (
    AccessDenied,
    BadDigest,
    BucketAlreadyExists,
    BucketNotEmpty,
    InvalidAccessKeyId,
    InvalidArgument,
    InvalidBucketName,
    InvalidObjectName,
    NoSuchBucket,
    NoSuchKey,
    NoSuchUpload,
    S3Error,
    SignatureDoesNotMatch,
)


def test_default_message_when_no_args():
    err = NoSuchKey()
    assert err.code == "NoSuchKey"
    assert err.http_status == 404
    assert err.message_text == NoSuchKey.message


def test_custom_message_overrides_default():
    err = NoSuchKey("my-key", resource="bucket/my-key")
    assert err.message_text == "my-key"
    assert err.resource == "bucket/my-key"


def test_to_xml_contains_required_fields():
    err = NoSuchBucket("widgets", resource="widgets")
    xml = err.to_xml().decode("utf-8")
    assert xml.startswith('<?xml version="1.0" encoding="UTF-8"?>')
    assert "<Code>NoSuchBucket</Code>" in xml
    assert "<Message>widgets</Message>" in xml
    assert "<Resource>widgets</Resource>" in xml
    assert "<RequestId>" in xml


def test_to_xml_escapes_special_chars():
    err = NoSuchKey("a < b & c > d", resource="bucket/a<b")
    xml = err.to_xml().decode("utf-8")
    assert "a &lt; b &amp; c &gt; d" in xml
    assert "a&lt;b" in xml


def test_status_codes():
    assert AccessDenied().http_status == 403
    assert InvalidAccessKeyId().http_status == 403
    assert SignatureDoesNotMatch().http_status == 403
    assert NoSuchBucket().http_status == 404
    assert NoSuchKey().http_status == 404
    assert NoSuchUpload().http_status == 404
    assert BucketAlreadyExists().http_status == 409
    assert BucketNotEmpty().http_status == 409
    assert InvalidBucketName().http_status == 400
    assert InvalidObjectName().http_status == 400
    assert InvalidArgument().http_status == 400
    assert BadDigest().http_status == 400


def test_all_inherit_from_s3error():
    assert issubclass(NoSuchKey, S3Error)
    assert issubclass(BadDigest, S3Error)
