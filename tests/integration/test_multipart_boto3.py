"""Wire-compatibility tests for the multipart upload endpoints via boto3.

These exercise the full create / upload-part / complete / list cycle that
boto3's `upload_fileobj` and `MultipartUpload` resource use under the hood.
"""

from __future__ import annotations

import io
import uuid

import pytest
from botocore.exceptions import ClientError


def _bucket_name() -> str:
    return f"nanio-mp-{uuid.uuid4().hex[:12]}"


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


def test_create_and_abort(boto3_client, bucket):
    create = boto3_client.create_multipart_upload(Bucket=bucket, Key="big.bin")
    upload_id = create["UploadId"]
    assert upload_id
    boto3_client.abort_multipart_upload(Bucket=bucket, Key="big.bin", UploadId=upload_id)


def test_upload_three_parts_and_complete(boto3_client, bucket):
    create = boto3_client.create_multipart_upload(
        Bucket=bucket, Key="big.bin", ContentType="application/octet-stream"
    )
    upload_id = create["UploadId"]

    part_data = [b"a" * (5 * 1024 * 1024), b"b" * (5 * 1024 * 1024), b"c" * 1024]
    parts_for_complete = []
    for i, data in enumerate(part_data, start=1):
        resp = boto3_client.upload_part(
            Bucket=bucket,
            Key="big.bin",
            UploadId=upload_id,
            PartNumber=i,
            Body=data,
        )
        parts_for_complete.append({"ETag": resp["ETag"], "PartNumber": i})

    complete = boto3_client.complete_multipart_upload(
        Bucket=bucket,
        Key="big.bin",
        UploadId=upload_id,
        MultipartUpload={"Parts": parts_for_complete},
    )
    # Multipart ETag must be `<hex>-3`.
    assert complete["ETag"].endswith('-3"')

    # The object is now downloadable as a normal GetObject.
    head = boto3_client.head_object(Bucket=bucket, Key="big.bin")
    assert head["ContentLength"] == sum(len(p) for p in part_data)


def test_list_parts(boto3_client, bucket):
    create = boto3_client.create_multipart_upload(Bucket=bucket, Key="k")
    upload_id = create["UploadId"]
    boto3_client.upload_part(Bucket=bucket, Key="k", UploadId=upload_id, PartNumber=1, Body=b"x")
    boto3_client.upload_part(Bucket=bucket, Key="k", UploadId=upload_id, PartNumber=2, Body=b"y")
    parts = boto3_client.list_parts(Bucket=bucket, Key="k", UploadId=upload_id)
    assert [p["PartNumber"] for p in parts.get("Parts", [])] == [1, 2]
    boto3_client.abort_multipart_upload(Bucket=bucket, Key="k", UploadId=upload_id)


def test_list_multipart_uploads(boto3_client, bucket):
    c1 = boto3_client.create_multipart_upload(Bucket=bucket, Key="a")
    c2 = boto3_client.create_multipart_upload(Bucket=bucket, Key="b")
    listed = boto3_client.list_multipart_uploads(Bucket=bucket)
    keys = sorted(u["Key"] for u in listed.get("Uploads", []))
    assert "a" in keys and "b" in keys
    boto3_client.abort_multipart_upload(Bucket=bucket, Key="a", UploadId=c1["UploadId"])
    boto3_client.abort_multipart_upload(Bucket=bucket, Key="b", UploadId=c2["UploadId"])


def test_upload_fileobj_uses_multipart_for_large_files(boto3_client, bucket):
    """boto3.upload_fileobj automatically uses multipart for files over the threshold."""
    from boto3.s3.transfer import TransferConfig

    # Force multipart by setting a tiny threshold.
    config = TransferConfig(
        multipart_threshold=5 * 1024 * 1024,
        multipart_chunksize=5 * 1024 * 1024,
        max_concurrency=2,
    )
    payload = b"x" * (12 * 1024 * 1024)  # 12 MB → 3 parts
    boto3_client.upload_fileobj(
        io.BytesIO(payload),
        bucket,
        "big.bin",
        Config=config,
    )
    head = boto3_client.head_object(Bucket=bucket, Key="big.bin")
    assert head["ContentLength"] == len(payload)
    assert head["ETag"].endswith('-3"')

    # Round-trip download
    out = io.BytesIO()
    boto3_client.download_fileobj(bucket, "big.bin", out, Config=config)
    assert out.getvalue() == payload
