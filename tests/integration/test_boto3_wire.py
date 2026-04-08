"""Wire-compatibility tests using the real `boto3` client against a live nanio.

This is the contract that nanio must hold: every operation here is exactly
what an unmodified S3 client running against AWS would do, and they must
all succeed against nanio with no special-casing on the client side.

Each test uses a fresh, uniquely-named bucket so the live server can be
shared across the whole test session.
"""

from __future__ import annotations

import io
import uuid

import pytest
from botocore.exceptions import ClientError


def _bucket_name() -> str:
    return f"nanio-test-{uuid.uuid4().hex[:12]}"


@pytest.fixture
def bucket(boto3_client):
    name = _bucket_name()
    boto3_client.create_bucket(Bucket=name)
    yield name
    # best-effort cleanup
    try:
        for obj in boto3_client.list_objects_v2(Bucket=name).get("Contents", []) or []:
            boto3_client.delete_object(Bucket=name, Key=obj["Key"])
        boto3_client.delete_bucket(Bucket=name)
    except ClientError:
        pass


# ----------------------------------------------------------------------
# Bucket-level
# ----------------------------------------------------------------------


def test_list_buckets(boto3_client, bucket):
    resp = boto3_client.list_buckets()
    names = [b["Name"] for b in resp.get("Buckets", [])]
    assert bucket in names


def test_head_bucket_existing(boto3_client, bucket):
    resp = boto3_client.head_bucket(Bucket=bucket)
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_head_bucket_missing(boto3_client):
    with pytest.raises(ClientError) as ei:
        boto3_client.head_bucket(Bucket=_bucket_name())
    assert ei.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_delete_empty_bucket(boto3_client):
    name = _bucket_name()
    boto3_client.create_bucket(Bucket=name)
    boto3_client.delete_bucket(Bucket=name)
    with pytest.raises(ClientError):
        boto3_client.head_bucket(Bucket=name)


# ----------------------------------------------------------------------
# Object PUT/GET/HEAD/DELETE
# ----------------------------------------------------------------------


def test_put_get_object(boto3_client, bucket):
    payload = b"hello from boto3"
    boto3_client.put_object(Bucket=bucket, Key="hello.txt", Body=payload)
    resp = boto3_client.get_object(Bucket=bucket, Key="hello.txt")
    assert resp["Body"].read() == payload
    assert resp["ContentLength"] == len(payload)


def test_put_with_content_type_and_metadata(boto3_client, bucket):
    boto3_client.put_object(
        Bucket=bucket,
        Key="meta.json",
        Body=b"{}",
        ContentType="application/json",
        Metadata={"author": "alice", "purpose": "test"},
    )
    head = boto3_client.head_object(Bucket=bucket, Key="meta.json")
    assert head["ContentType"] == "application/json"
    assert head["Metadata"]["author"] == "alice"
    assert head["Metadata"]["purpose"] == "test"


def test_get_missing_key(boto3_client, bucket):
    with pytest.raises(ClientError) as ei:
        boto3_client.get_object(Bucket=bucket, Key="nope")
    assert ei.value.response["Error"]["Code"] == "NoSuchKey"


def test_delete_object(boto3_client, bucket):
    boto3_client.put_object(Bucket=bucket, Key="k", Body=b"x")
    boto3_client.delete_object(Bucket=bucket, Key="k")
    with pytest.raises(ClientError):
        boto3_client.head_object(Bucket=bucket, Key="k")


def test_get_with_range(boto3_client, bucket):
    boto3_client.put_object(Bucket=bucket, Key="k", Body=b"abcdefghij")
    resp = boto3_client.get_object(Bucket=bucket, Key="k", Range="bytes=2-5")
    assert resp["Body"].read() == b"cdef"
    assert resp["ContentRange"] == "bytes 2-5/10"


def test_put_object_with_subkey(boto3_client, bucket):
    boto3_client.put_object(Bucket=bucket, Key="path/to/deep/file.txt", Body=b"deep")
    resp = boto3_client.get_object(Bucket=bucket, Key="path/to/deep/file.txt")
    assert resp["Body"].read() == b"deep"


def test_put_streaming_body(boto3_client, bucket):
    payload = b"hello world" * 1024  # 11 KB
    boto3_client.put_object(Bucket=bucket, Key="k", Body=io.BytesIO(payload))
    resp = boto3_client.get_object(Bucket=bucket, Key="k")
    assert resp["Body"].read() == payload


# ----------------------------------------------------------------------
# Listing
# ----------------------------------------------------------------------


def test_list_objects_v2_basic(boto3_client, bucket):
    for k in ["a.txt", "b.txt", "c.txt"]:
        boto3_client.put_object(Bucket=bucket, Key=k, Body=b"x")
    resp = boto3_client.list_objects_v2(Bucket=bucket)
    keys = sorted(o["Key"] for o in resp["Contents"])
    assert keys == ["a.txt", "b.txt", "c.txt"]
    assert resp["KeyCount"] == 3
    assert resp["IsTruncated"] is False


def test_list_objects_v2_with_prefix(boto3_client, bucket):
    for k in ["logs/jan.txt", "logs/feb.txt", "data/raw.bin"]:
        boto3_client.put_object(Bucket=bucket, Key=k, Body=b"x")
    resp = boto3_client.list_objects_v2(Bucket=bucket, Prefix="logs/")
    keys = sorted(o["Key"] for o in resp["Contents"])
    assert keys == ["logs/feb.txt", "logs/jan.txt"]


def test_list_objects_v2_with_delimiter(boto3_client, bucket):
    for k in ["logs/jan.txt", "logs/feb.txt", "data/raw.bin", "top.txt"]:
        boto3_client.put_object(Bucket=bucket, Key=k, Body=b"x")
    resp = boto3_client.list_objects_v2(Bucket=bucket, Delimiter="/")
    keys = sorted(o["Key"] for o in resp.get("Contents", []))
    common = sorted(p["Prefix"] for p in resp.get("CommonPrefixes", []))
    assert keys == ["top.txt"]
    assert common == ["data/", "logs/"]


def test_list_objects_v2_pagination(boto3_client, bucket):
    for i in range(7):
        boto3_client.put_object(Bucket=bucket, Key=f"k-{i}.txt", Body=b"x")
    paginator = boto3_client.get_paginator("list_objects_v2")
    seen: list[str] = []
    for page in paginator.paginate(Bucket=bucket, PaginationConfig={"PageSize": 3}):
        seen.extend(o["Key"] for o in page.get("Contents", []))
    assert sorted(seen) == [f"k-{i}.txt" for i in range(7)]
