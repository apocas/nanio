"""Streaming I/O tests — assert that big uploads/downloads do not blow RSS.

These are slow (multi-GB) and gated behind `@pytest.mark.slow`. Run with:

    uv run pytest -m slow

The contract: nanio MUST stream request and response bodies. The memory
ceiling per concurrent request must remain a small constant (a few MB),
not scale with body size. We enforce this by uploading and downloading
a 1 GB body and asserting that the server process's RSS does not grow
by more than 200 MB.
"""

from __future__ import annotations

import contextlib
import io
import os
import time

import pytest
from botocore.exceptions import ClientError

SIZE_MB = int(os.environ.get("NANIO_STREAMING_TEST_MB", "1024"))  # default 1 GB


def _rss_kb_of(pid: int) -> int:
    """Return the resident set size (in KB) of `pid` from /proc."""
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except FileNotFoundError:
        pass
    return -1


class _GenStream(io.RawIOBase):
    """Read-only file-like object that yields `total` bytes of `b'x'` on demand.

    This avoids ever materializing the test payload in memory.
    """

    def __init__(self, total: int) -> None:
        self._total = total
        self._sent = 0
        # boto3 inspects size via `len(file)` or `seek(0, 2); tell()`. Provide both.

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        if self._sent >= self._total:
            return b""
        if size is None or size < 0:
            size = self._total - self._sent
        size = min(size, self._total - self._sent)
        self._sent += size
        return b"x" * size

    def __len__(self) -> int:
        return self._total

    def seek(self, offset: int, whence: int = 0) -> int:
        if whence == 2:
            self._sent = self._total
        elif whence == 0:
            self._sent = offset
        elif whence == 1:
            self._sent += offset
        return self._sent

    def tell(self) -> int:
        return self._sent


@pytest.mark.slow
def test_streaming_upload_and_download_bounded_memory(boto3_client, live_server):
    """Upload + download `SIZE_MB` MB and assert server RSS stays bounded."""
    bucket = "nanio-streaming-test"
    with contextlib.suppress(ClientError):
        boto3_client.create_bucket(Bucket=bucket)

    baseline = _rss_kb_of(live_server.pid)
    assert baseline > 0, "could not read /proc/<pid>/status — Linux only"

    total = SIZE_MB * 1024 * 1024
    stream = _GenStream(total)

    t0 = time.monotonic()
    boto3_client.put_object(Bucket=bucket, Key="big.bin", Body=stream, ContentLength=total)
    upload_seconds = time.monotonic() - t0

    after_upload = _rss_kb_of(live_server.pid)

    # Now download and discard.
    t0 = time.monotonic()
    resp = boto3_client.get_object(Bucket=bucket, Key="big.bin")
    body = resp["Body"]
    received = 0
    while True:
        chunk = body.read(64 * 1024)
        if not chunk:
            break
        received += len(chunk)
    download_seconds = time.monotonic() - t0
    assert received == total

    after_download = _rss_kb_of(live_server.pid)

    growth_upload = (after_upload - baseline) / 1024  # MB
    growth_download = (after_download - baseline) / 1024  # MB

    print(
        f"\nstreaming test: {SIZE_MB} MB, "
        f"baseline={baseline / 1024:.1f} MB, "
        f"after_upload={after_upload / 1024:.1f} MB (+{growth_upload:.1f} MB), "
        f"after_download={after_download / 1024:.1f} MB (+{growth_download:.1f} MB), "
        f"upload={upload_seconds:.1f}s, download={download_seconds:.1f}s"
    )

    # Server-side RSS must not grow proportionally to body size.
    assert growth_upload < 200, f"RSS grew by {growth_upload:.1f} MB during upload"
    assert growth_download < 200, f"RSS grew by {growth_download:.1f} MB during download"

    # Cleanup
    boto3_client.delete_object(Bucket=bucket, Key="big.bin")
    boto3_client.delete_bucket(Bucket=bucket)
