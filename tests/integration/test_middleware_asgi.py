"""In-process ASGI tests for `SigV4Middleware`.

The existing wire-compat suite (`test_boto3_wire.py`) exercises the
middleware end-to-end but via a uvicorn subprocess, so in-process
coverage never sees the middleware's own code. These tests send
signed requests through the app via `httpx.ASGITransport`, which runs
the middleware inside the pytest process and counts toward coverage.

Two flavors:

1. Header-form SigV4, signed with boto3's own `S3SigV4Auth`. Covers the
   regular `verify_header_auth` path + the non-streaming fast-exit.
2. `STREAMING-AWS4-HMAC-SHA256-PAYLOAD`, signed manually using nanio's
   own primitives (we've proven in `test_sigv4.py` that they match
   boto3 bit-for-bit on the seed signature). Covers
   `_make_chunked_receive`, the streaming branch of `__call__`, and
   every line of `auth/chunked.py` that end-to-end uses.
"""

from __future__ import annotations

import hashlib
import hmac
from datetime import UTC, datetime
from urllib.parse import quote

import pytest
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from httpx import ASGITransport, AsyncClient

from nanio.app import build_app
from nanio.auth.chunked import CHUNK_ALGORITHM
from nanio.auth.credentials import StaticCredentialResolver
from nanio.auth.sigv4 import (
    ALGORITHM,
    EMPTY_SHA256,
    STREAMING_PAYLOAD,
    build_canonical_request,
    build_string_to_sign,
    compute_signature,
    derive_signing_key,
)
from nanio.config import Settings

ACCESS_KEY = "AKIDEXAMPLE"
SECRET_KEY = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
REGION = "us-east-1"


@pytest.fixture
def auth_enabled_settings(tmp_path):
    """A `Settings` with SigV4 verification ACTIVE (auth_disabled=False)."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    return Settings(
        data_dir=data_dir,
        credentials=StaticCredentialResolver({ACCESS_KEY: SECRET_KEY}),
        auth_disabled=False,
    )


@pytest.fixture
def auth_enabled_app(auth_enabled_settings):
    return build_app(auth_enabled_settings)


@pytest.fixture
async def auth_enabled_client(auth_enabled_app):
    transport = ASGITransport(app=auth_enabled_app)
    async with AsyncClient(transport=transport, base_url="http://nanio.test") as client:
        yield client


# ----------------------------------------------------------------------
# Header-form SigV4 via boto3 signer
# ----------------------------------------------------------------------


def _sign_with_boto3(method: str, url: str, body: bytes = b"") -> AWSRequest:
    creds = Credentials(ACCESS_KEY, SECRET_KEY)
    req = AWSRequest(method=method, url=url, data=body, headers={})
    req.headers["x-amz-content-sha256"] = hashlib.sha256(body).hexdigest()
    S3SigV4Auth(creds, "s3", REGION).add_auth(req)
    return req


async def _send_signed(client: AsyncClient, method: str, path: str, body: bytes = b""):
    url = f"http://nanio.test{path}"
    signed = _sign_with_boto3(method, url, body)
    headers = dict(signed.headers)
    headers["host"] = "nanio.test"
    return await client.request(method, path, headers=headers, content=body)


@pytest.mark.asyncio
async def test_middleware_accepts_valid_header_sigv4(auth_enabled_client):
    r = await _send_signed(auth_enabled_client, "GET", "/")
    assert r.status_code == 200
    assert b"ListAllMyBucketsResult" in r.content


@pytest.mark.asyncio
async def test_middleware_rejects_missing_authorization(auth_enabled_client):
    r = await auth_enabled_client.get("/")
    assert r.status_code == 403
    assert b"MissingAuthenticationToken" in r.content


@pytest.mark.asyncio
async def test_middleware_rejects_tampered_signature(auth_enabled_client):
    signed = _sign_with_boto3("GET", "http://nanio.test/")
    headers = dict(signed.headers)
    headers["host"] = "nanio.test"
    # Flip one hex char at the end of the signature.
    auth = headers["Authorization"]
    headers["Authorization"] = auth[:-1] + ("0" if auth[-1] != "0" else "1")
    r = await auth_enabled_client.get("/", headers=headers)
    assert r.status_code == 403
    assert b"SignatureDoesNotMatch" in r.content


@pytest.mark.asyncio
async def test_middleware_rejects_unknown_access_key(auth_enabled_client):
    """Sign with a valid key, then rewrite the credential scope to point
    at a different access key the resolver doesn't know."""
    signed = _sign_with_boto3("GET", "http://nanio.test/")
    headers = dict(signed.headers)
    headers["host"] = "nanio.test"
    headers["Authorization"] = headers["Authorization"].replace(
        f"Credential={ACCESS_KEY}/",
        "Credential=UNKNOWN_KEY/",
    )
    r = await auth_enabled_client.get("/", headers=headers)
    assert r.status_code == 403
    assert b"InvalidAccessKeyId" in r.content


# ----------------------------------------------------------------------
# Presigned URL (query-string SigV4)
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_middleware_accepts_valid_presigned_url(auth_enabled_client):
    """Construct a presigned GET URL manually, covering the query-string
    branch of `_verify`."""
    # Use our own primitives to build a valid presigned query.
    amz_date = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    date = amz_date[:8]
    scope = f"{date}/{REGION}/s3/aws4_request"

    qparams = {
        "X-Amz-Algorithm": ALGORITHM,
        "X-Amz-Credential": f"{ACCESS_KEY}/{scope}",
        "X-Amz-Date": amz_date,
        "X-Amz-Expires": "600",
        "X-Amz-SignedHeaders": "host",
    }
    # Match the encoding that `verify_presigned_url` applies when
    # rebuilding the canonical query string (`quote(safe='-_.~')`).
    canon_query = "&".join(
        f"{quote(k, safe='-_.~')}={quote(v, safe='-_.~')}" for k, v in sorted(qparams.items())
    )

    headers_lower = {"host": "nanio.test"}
    canonical = build_canonical_request(
        method="GET",
        path="/",
        query=canon_query,
        headers=headers_lower,
        signed_headers=["host"],
        payload_hash="UNSIGNED-PAYLOAD",
    )
    sts = build_string_to_sign(
        amz_date=amz_date, credential_scope=scope, canonical_request=canonical
    )
    signing_key = derive_signing_key(SECRET_KEY, date=date, region=REGION, service="s3")
    sig = compute_signature(signing_key, sts)
    qparams["X-Amz-Signature"] = sig

    # Send the query unencoded — the verifier will re-encode when
    # rebuilding the canonical form.
    query = "&".join(f"{k}={v}" for k, v in qparams.items())
    r = await auth_enabled_client.get(f"/?{query}")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_middleware_rejects_presigned_with_bad_signature(auth_enabled_client):
    """A presigned URL with a wrong signature must be rejected."""
    amz_date = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    date = amz_date[:8]
    scope = f"{date}/{REGION}/s3/aws4_request"
    query = (
        f"X-Amz-Algorithm={ALGORITHM}"
        f"&X-Amz-Credential={ACCESS_KEY}/{scope}"
        f"&X-Amz-Date={amz_date}"
        "&X-Amz-Expires=600"
        "&X-Amz-SignedHeaders=host"
        "&X-Amz-Signature=" + "0" * 64
    )
    r = await auth_enabled_client.get(f"/?{query}")
    assert r.status_code == 403
    assert b"SignatureDoesNotMatch" in r.content


# ----------------------------------------------------------------------
# Streaming SigV4 (STREAMING-AWS4-HMAC-SHA256-PAYLOAD)
# ----------------------------------------------------------------------


def _build_streaming_payload_request(
    *, method: str, path: str, body: bytes, decoded_length: int, chunk_size: int = 4096
) -> tuple[dict[str, str], bytes]:
    """Construct a complete STREAMING-AWS4-HMAC-SHA256-PAYLOAD request.

    Returns (headers, encoded_body) ready to pass to an httpx client.
    Uses our own nanio primitives (not boto3) because boto3 does not
    natively emit this encoding for unmodified uploads.
    """
    amz_date = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    date = amz_date[:8]
    scope = f"{date}/{REGION}/s3/aws4_request"
    signing_key = derive_signing_key(SECRET_KEY, date=date, region=REGION, service="s3")

    # Chunk the body up.
    chunks: list[bytes] = []
    pos = 0
    while pos < len(body):
        chunks.append(body[pos : pos + chunk_size])
        pos += chunk_size

    # Build the aws-chunked encoded body.
    # Compute the encoded body length for the Content-Length header — the
    # client is supposed to set this to the length of the wire body,
    # including the framing. But for our in-process test we only need
    # the headers to be self-consistent with the canonical request, so
    # we can leave Content-Length out and rely on more_body framing.
    signed_headers_list = [
        "content-encoding",
        "host",
        "x-amz-content-sha256",
        "x-amz-date",
        "x-amz-decoded-content-length",
    ]
    headers_lower = {
        "content-encoding": "aws-chunked",
        "host": "nanio.test",
        "x-amz-content-sha256": STREAMING_PAYLOAD,
        "x-amz-date": amz_date,
        "x-amz-decoded-content-length": str(decoded_length),
    }

    canon = build_canonical_request(
        method=method,
        path=path,
        query="",
        headers=headers_lower,
        signed_headers=signed_headers_list,
        payload_hash=STREAMING_PAYLOAD,
    )
    sts = build_string_to_sign(amz_date=amz_date, credential_scope=scope, canonical_request=canon)
    seed_signature = compute_signature(signing_key, sts)

    auth_header = (
        f"{ALGORITHM} Credential={ACCESS_KEY}/{scope}, "
        f"SignedHeaders={';'.join(signed_headers_list)}, "
        f"Signature={seed_signature}"
    )

    # Build the chunked body.
    prev_sig = seed_signature
    framed = bytearray()
    for chunk in chunks:
        chunk_sig = _sign_chunk_for_test(signing_key, amz_date, scope, prev_sig, chunk)
        framed.extend(f"{len(chunk):x};chunk-signature={chunk_sig}\r\n".encode("ascii"))
        framed.extend(chunk)
        framed.extend(b"\r\n")
        prev_sig = chunk_sig

    final_sig = _sign_chunk_for_test(signing_key, amz_date, scope, prev_sig, b"")
    framed.extend(f"0;chunk-signature={final_sig}\r\n\r\n".encode("ascii"))

    headers = {
        **{k: v for k, v in headers_lower.items()},
        "Authorization": auth_header,
    }
    return headers, bytes(framed)


def _sign_chunk_for_test(
    signing_key: bytes, amz_date: str, scope: str, prev_sig: str, chunk: bytes
) -> str:
    chunk_sha = hashlib.sha256(chunk).hexdigest()
    sts = "\n".join([CHUNK_ALGORITHM, amz_date, scope, prev_sig, EMPTY_SHA256, chunk_sha])
    return hmac.new(signing_key, sts.encode(), hashlib.sha256).hexdigest()


@pytest.mark.asyncio
async def test_middleware_accepts_streaming_payload_put(auth_enabled_client):
    """End-to-end streaming PUT that exercises `_make_chunked_receive`
    and the entire streaming decode path in `auth/chunked.py`."""
    # First create the bucket via a normal header-signed request.
    r = await _send_signed(auth_enabled_client, "PUT", "/widgets")
    assert r.status_code == 200

    # Now upload an object using STREAMING-AWS4-HMAC-SHA256-PAYLOAD.
    payload = b"hello streaming world!" * 100
    headers, framed = _build_streaming_payload_request(
        method="PUT",
        path="/widgets/streamed.bin",
        body=payload,
        decoded_length=len(payload),
        chunk_size=32,  # force multiple chunks
    )
    r = await auth_enabled_client.put("/widgets/streamed.bin", headers=headers, content=framed)
    assert r.status_code == 200

    # Verify the server stored the decoded bytes, not the framed ones.
    r = await _send_signed(auth_enabled_client, "GET", "/widgets/streamed.bin")
    assert r.status_code == 200
    assert r.content == payload
