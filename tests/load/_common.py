"""Shared helpers for nanio locust load tests.

Locust calls user methods on each tick. We avoid creating a new boto3
client per request — boto3 client construction is expensive (parses
endpoints.json, builds models, etc.). Instead, each User stores one
boto3 client at startup and reuses it.

Run a load test against a local nanio with:

    NANIO_ACCESS_KEY=test NANIO_SECRET_KEY=test \\
        nanio serve --data-dir /tmp/nanio-loadtest --port 9000 &

    uv run --extra loadtest locust \\
        -f tests/load/locustfile_small.py \\
        --host http://127.0.0.1:9000 \\
        --headless -u 50 -r 10 -t 60s
"""

from __future__ import annotations

import contextlib
import os
import uuid

DEFAULT_ACCESS_KEY = os.environ.get("NANIO_LOAD_ACCESS_KEY", "test")
DEFAULT_SECRET_KEY = os.environ.get("NANIO_LOAD_SECRET_KEY", "test")
DEFAULT_REGION = os.environ.get("NANIO_LOAD_REGION", "us-east-1")
LOAD_BUCKET = os.environ.get("NANIO_LOAD_BUCKET", "loadtest")


def make_client(host: str):
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=host,
        aws_access_key_id=DEFAULT_ACCESS_KEY,
        aws_secret_access_key=DEFAULT_SECRET_KEY,
        region_name=DEFAULT_REGION,
        config=Config(
            s3={"addressing_style": "path", "payload_signing_enabled": False},
            signature_version="s3v4",
            retries={"max_attempts": 1, "mode": "standard"},
        ),
    )


def ensure_bucket(client, name: str = LOAD_BUCKET) -> None:
    try:
        client.head_bucket(Bucket=name)
    except Exception:
        with contextlib.suppress(Exception):
            client.create_bucket(Bucket=name)


def random_key(prefix: str = "obj") -> str:
    return f"{prefix}-{uuid.uuid4().hex}"
