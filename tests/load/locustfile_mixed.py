"""Locust load test: mixed read/write workload.

A more realistic mix:

- 60% GET an existing object
- 20% PUT a new object
- 10% LIST objects (paginated)
- 10% HEAD an existing object

The user pre-populates a small set of keys at startup so reads have
something to chew on.

    uv run --extra loadtest locust \\
        -f tests/load/locustfile_mixed.py \\
        --host http://127.0.0.1:9000 \\
        --headless -u 50 -r 10 -t 120s
"""

from __future__ import annotations

import random
import time

from locust import User, between, events, task

from tests.load._common import LOAD_BUCKET, ensure_bucket, make_client, random_key

PAYLOAD = b"y" * 4096  # 4 KB
SEED_KEYS = 50


class MixedUser(User):
    wait_time = between(0.0, 0.1)
    abstract = False

    def on_start(self):
        self.client_s3 = make_client(self.host)
        ensure_bucket(self.client_s3)
        self.keys = []
        for _ in range(SEED_KEYS):
            k = random_key("mix")
            self.client_s3.put_object(Bucket=LOAD_BUCKET, Key=k, Body=PAYLOAD)
            self.keys.append(k)

    @task(60)
    def get(self):
        key = random.choice(self.keys)
        self._timed(
            "GET mix",
            lambda: self.client_s3.get_object(Bucket=LOAD_BUCKET, Key=key)["Body"].read(),
            byte_count=len(PAYLOAD),
        )

    @task(20)
    def put(self):
        key = random_key("mix")
        self._timed(
            "PUT mix",
            lambda: self.client_s3.put_object(Bucket=LOAD_BUCKET, Key=key, Body=PAYLOAD),
            byte_count=len(PAYLOAD),
        )
        self.keys.append(key)
        if len(self.keys) > 200:
            self.keys.pop(0)

    @task(10)
    def list_page(self):
        self._timed(
            "LIST mix",
            lambda: self.client_s3.list_objects_v2(Bucket=LOAD_BUCKET, MaxKeys=100),
            byte_count=0,
        )

    @task(10)
    def head(self):
        key = random.choice(self.keys)
        self._timed(
            "HEAD mix",
            lambda: self.client_s3.head_object(Bucket=LOAD_BUCKET, Key=key),
            byte_count=0,
        )

    def _timed(self, name: str, fn, *, byte_count: int) -> None:
        t0 = time.perf_counter()
        try:
            fn()
            elapsed_ms = (time.perf_counter() - t0) * 1000
            events.request.fire(
                request_type="boto3",
                name=name,
                response_time=elapsed_ms,
                response_length=byte_count,
                exception=None,
                context={},
            )
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - t0) * 1000
            events.request.fire(
                request_type="boto3",
                name=name,
                response_time=elapsed_ms,
                response_length=0,
                exception=exc,
                context={},
            )
