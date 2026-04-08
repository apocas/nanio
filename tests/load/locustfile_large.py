"""Locust load test: large files via multipart upload.

Each user repeatedly uploads a 50 MB file using boto3's TransferConfig
(which automatically picks multipart for files over the threshold), then
downloads it, then deletes it.

Run headless:

    uv run --extra loadtest locust \\
        -f tests/load/locustfile_large.py \\
        --host http://127.0.0.1:9000 \\
        --headless -u 4 -r 1 -t 120s

Lower user counts are appropriate here — each iteration moves 100 MB.
"""

from __future__ import annotations

import io
import time

from boto3.s3.transfer import TransferConfig
from locust import User, between, events, task

from tests.load._common import LOAD_BUCKET, ensure_bucket, make_client, random_key

OBJECT_SIZE = 50 * 1024 * 1024  # 50 MB
TRANSFER_CONFIG = TransferConfig(
    multipart_threshold=8 * 1024 * 1024,
    multipart_chunksize=8 * 1024 * 1024,
    max_concurrency=2,
)


class LargeObjectUser(User):
    wait_time = between(0.5, 1.5)
    abstract = False

    def on_start(self):
        self.client_s3 = make_client(self.host)
        ensure_bucket(self.client_s3)
        self.payload = b"x" * OBJECT_SIZE

    @task
    def upload_download_delete(self):
        key = random_key("large")
        self._timed(
            "UPLOAD large (multipart)",
            lambda: self.client_s3.upload_fileobj(
                io.BytesIO(self.payload),
                LOAD_BUCKET,
                key,
                Config=TRANSFER_CONFIG,
            ),
            byte_count=OBJECT_SIZE,
        )
        sink = io.BytesIO()
        self._timed(
            "DOWNLOAD large",
            lambda: self.client_s3.download_fileobj(
                LOAD_BUCKET,
                key,
                sink,
                Config=TRANSFER_CONFIG,
            ),
            byte_count=OBJECT_SIZE,
        )
        self._timed(
            "DELETE large",
            lambda: self.client_s3.delete_object(Bucket=LOAD_BUCKET, Key=key),
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
