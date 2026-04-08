"""Spawns nanio in a real uvicorn subprocess so boto3 can talk to it.

We can't use httpx ASGITransport for boto3 — boto3 makes raw socket
connections via urllib3, so we need an actual TCP listener. The fixture
launches `python -m uvicorn` as a subprocess, waits for the port to be
ready, yields the endpoint URL, then SIGTERMs the process on teardown.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest


@dataclass(slots=True)
class LiveServer:
    endpoint_url: str
    access_key: str
    secret_key: str
    data_dir: Path
    pid: int


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_listener(host: str, port: int, deadline: float) -> None:
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.25):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"nanio did not become ready on {host}:{port}")


@pytest.fixture(scope="session")
def live_server(tmp_path_factory) -> Iterator[LiveServer]:
    data_dir = tmp_path_factory.mktemp("nanio-live-data")
    port = _free_port()
    access_key = "TESTACCESSKEY"
    secret_key = "TESTSECRETKEY1234567890"

    env = os.environ.copy()
    env["NANIO_ACCESS_KEY"] = access_key
    env["NANIO_SECRET_KEY"] = secret_key

    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "nanio.app:build_app",
            "--factory",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "warning",
        ],
        env={**env, "NANIO_DATA_DIR": str(data_dir)},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_for_listener("127.0.0.1", port, deadline=time.monotonic() + 10.0)
        yield LiveServer(
            endpoint_url=f"http://127.0.0.1:{port}",
            access_key=access_key,
            secret_key=secret_key,
            data_dir=data_dir,
            pid=proc.pid,
        )
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


@pytest.fixture
def boto3_client(live_server: LiveServer):
    """Per-test boto3 S3 client wired to the live server.

    Each test gets a fresh client (cheap) but shares the long-running server
    across the session. Tests that need data isolation should use unique
    bucket names.
    """
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=live_server.endpoint_url,
        aws_access_key_id=live_server.access_key,
        aws_secret_access_key=live_server.secret_key,
        region_name="us-east-1",
        config=Config(
            s3={"addressing_style": "path", "payload_signing_enabled": False},
            signature_version="s3v4",
        ),
    )
