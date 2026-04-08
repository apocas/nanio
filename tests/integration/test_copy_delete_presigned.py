"""Integration tests for CopyObject, DeleteObjects batch, and presigned URLs."""

from __future__ import annotations

import contextlib
import uuid

import pytest
import requests
from botocore.exceptions import ClientError


def _bucket_name() -> str:
    return f"nanio-cdp-{uuid.uuid4().hex[:12]}"


@pytest.fixture
def bucket(boto3_client):
    name = _bucket_name()
    boto3_client.create_bucket(Bucket=name)
    yield name
    try:
        for obj in boto3_client.list_objects_v2(Bucket=name).get("Contents", []) or []:
            boto3_client.delete_object(Bucket=name, Key=obj["Key"])
        boto3_client.delete_bucket(Bucket=name)
    except ClientError:
        pass


# ----------------------------------------------------------------------
# CopyObject
# ----------------------------------------------------------------------


def test_copy_object_same_bucket(boto3_client, bucket):
    boto3_client.put_object(Bucket=bucket, Key="src", Body=b"hello", ContentType="text/plain")
    boto3_client.copy_object(
        Bucket=bucket,
        Key="dst",
        CopySource={"Bucket": bucket, "Key": "src"},
    )
    head = boto3_client.head_object(Bucket=bucket, Key="dst")
    assert head["ContentLength"] == 5
    assert head["ContentType"] == "text/plain"


def test_copy_object_across_buckets(boto3_client, bucket):
    other = _bucket_name()
    boto3_client.create_bucket(Bucket=other)
    try:
        boto3_client.put_object(Bucket=bucket, Key="src", Body=b"data")
        boto3_client.copy_object(
            Bucket=other,
            Key="dst",
            CopySource={"Bucket": bucket, "Key": "src"},
        )
        head = boto3_client.head_object(Bucket=other, Key="dst")
        assert head["ContentLength"] == 4
    finally:
        for k in ("dst",):
            with contextlib.suppress(ClientError):
                boto3_client.delete_object(Bucket=other, Key=k)
        boto3_client.delete_bucket(Bucket=other)


def test_copy_object_missing_source(boto3_client, bucket):
    with pytest.raises(ClientError):
        boto3_client.copy_object(
            Bucket=bucket,
            Key="dst",
            CopySource={"Bucket": bucket, "Key": "missing"},
        )


# ----------------------------------------------------------------------
# DeleteObjects batch
# ----------------------------------------------------------------------


def test_delete_objects_batch(boto3_client, bucket):
    for k in ["a", "b", "c", "d"]:
        boto3_client.put_object(Bucket=bucket, Key=k, Body=b"x")
    resp = boto3_client.delete_objects(
        Bucket=bucket,
        Delete={"Objects": [{"Key": "a"}, {"Key": "b"}, {"Key": "missing"}]},
    )
    deleted_keys = sorted(d["Key"] for d in resp.get("Deleted", []))
    # `missing` is also reported as Deleted in S3 (delete is idempotent).
    assert "a" in deleted_keys and "b" in deleted_keys

    # `c` and `d` should still exist.
    keys = [o["Key"] for o in boto3_client.list_objects_v2(Bucket=bucket)["Contents"]]
    assert sorted(keys) == ["c", "d"]


def test_delete_objects_batch_only_some_exist(boto3_client, bucket):
    boto3_client.put_object(Bucket=bucket, Key="exists", Body=b"x")
    resp = boto3_client.delete_objects(
        Bucket=bucket,
        Delete={
            "Objects": [
                {"Key": "exists"},
                {"Key": "ghost-1"},
                {"Key": "ghost-2"},
            ]
        },
    )
    deleted_keys = sorted(d["Key"] for d in resp.get("Deleted", []))
    # All three should be reported as deleted (S3 delete is idempotent).
    assert deleted_keys == ["exists", "ghost-1", "ghost-2"]


# ----------------------------------------------------------------------
# Presigned URLs
# ----------------------------------------------------------------------


def test_presigned_get_url_works(boto3_client, bucket, live_server):
    boto3_client.put_object(Bucket=bucket, Key="k", Body=b"hello")
    url = boto3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": "k"},
        ExpiresIn=600,
    )
    # Use plain `requests` so we don't accidentally re-sign.
    resp = requests.get(url)
    assert resp.status_code == 200
    assert resp.content == b"hello"


def test_presigned_get_url_invalid_signature_rejected(boto3_client, bucket):
    boto3_client.put_object(Bucket=bucket, Key="k", Body=b"hello")
    url = boto3_client.generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": "k"}, ExpiresIn=600
    )
    # Tamper with the signature
    bad = url[:-2] + ("00" if not url.endswith("00") else "11")
    resp = requests.get(bad)
    assert resp.status_code == 403


def test_presigned_put_url_works(boto3_client, bucket):
    url = boto3_client.generate_presigned_url(
        "put_object",
        Params={"Bucket": bucket, "Key": "k"},
        ExpiresIn=600,
    )
    resp = requests.put(url, data=b"hello via presigned")
    assert resp.status_code == 200
    head = boto3_client.head_object(Bucket=bucket, Key="k")
    assert head["ContentLength"] == len(b"hello via presigned")
