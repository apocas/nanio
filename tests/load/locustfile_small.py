"""Locust load test: many concurrent small-object PUT/GET/DELETE.

This is the "millions of small files" workload. Each user repeatedly puts
a 1 KB object, gets it back, then deletes it. Tune `wait_time` and run
duration via locust flags.

Run headless:

    uv run --extra loadtest locust \\
        -f tests/load/locustfile_small.py \\
        --host http://127.0.0.1:9000 \\
        --headless -u 100 -r 20 -t 60s
"""

from __future__ import annotations

import time

from locust import User, between, events, task

from tests.load._common import LOAD_BUCKET, ensure_bucket, make_client, random_key

PAYLOAD_SIZE = 1024  # 1 KB
PAYLOAD = b"x" * PAYLOAD_SIZE


class SmallObjectUser(User):
    wait_time = between(0.0, 0.05)
    abstract = False

    def on_start(self):
        self.client_s3 = make_client(self.host)
        ensure_bucket(self.client_s3)

    @task(3)
    def put_then_get(self):
        key = random_key("small")
        self._timed("PUT small", lambda: self.client_s3.put_object(Bucket=LOAD_BUCKET, Key=key, Body=PAYLOAD))
        self._timed("GET small", lambda: self.client_s3.get_object(Bucket=LOAD_BUCKET, Key=key)["Body"].read())
        self._timed("DELETE small", lambda: self.client_s3.delete_object(Bucket=LOAD_BUCKET, Key=key))

    @task(1)
    def list_first_page(self):
        self._timed("LIST", lambda: self.client_s3.list_objects_v2(Bucket=LOAD_BUCKET, MaxKeys=100))

    def _timed(self, name: str, fn):
        t0 = time.perf_counter()
        try:
            result = fn()
            elapsed_ms = (time.perf_counter() - t0) * 1000
            events.request.fire(
                request_type="boto3",
                name=name,
                response_time=elapsed_ms,
                response_length=PAYLOAD_SIZE,
                exception=None,
                context={},
            )
            return result
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
