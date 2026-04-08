"""SigV4 verification — server side.

Implements AWS Signature Version 4 verification from the spec at
https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html

Why we wrote it ourselves rather than using the `awssig` PyPI package:
1. `awssig` depends on `six`, which is end-of-life.
2. `awssig`'s API takes the full body as bytes — incompatible with our
   streaming model where the body might never fit in memory.

We don't need the body to verify a signature: the client always tells us
the SHA-256 of the body in the `x-amz-content-sha256` header. The verifier
trusts that header. If the client lies, the signature won't match.

For requests sent with `x-amz-content-sha256: STREAMING-AWS4-HMAC-SHA256-PAYLOAD`,
the chunked body itself carries per-chunk signatures that we verify
incrementally in `auth/chunked.py`. The verifier just confirms the header
signature for the request envelope.
"""

from __future__ import annotations

import hashlib
import hmac
from datetime import UTC, datetime, timedelta
from typing import NamedTuple
from urllib.parse import quote, unquote

from nanio.errors import (
    AuthorizationHeaderMalformed,
    InvalidAccessKeyId,
    MissingAuthenticationToken,
    RequestTimeTooSkewed,
    SignatureDoesNotMatch,
)

ALGORITHM = "AWS4-HMAC-SHA256"
SERVICE = "s3"
EMPTY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD"
STREAMING_PAYLOAD = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
STREAMING_PAYLOAD_TRAILER = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"

# Default clock-skew tolerance in seconds (matches AWS spec's 15 minutes,
# applied symmetrically).
DEFAULT_SKEW_SECONDS = 15 * 60


class AuthorizationParts(NamedTuple):
    """Parsed pieces of the `Authorization` header."""

    access_key: str
    date: str  # YYYYMMDD
    region: str
    service: str
    signed_headers: list[str]
    signature: str
    credential_scope: str  # date/region/service/aws4_request


def parse_authorization_header(value: str) -> AuthorizationParts:
    """Parse an `Authorization: AWS4-HMAC-SHA256 ...` header.

    Format:
        AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/s3/aws4_request,
        SignedHeaders=host;x-amz-date, Signature=abcdef...
    """
    if not value or not value.startswith(ALGORITHM + " "):
        raise AuthorizationHeaderMalformed(f"expected algorithm {ALGORITHM}, got {value!r}")
    body = value[len(ALGORITHM) + 1 :]
    fields: dict[str, str] = {}
    for chunk in body.split(","):
        chunk = chunk.strip()
        if not chunk or "=" not in chunk:
            continue
        k, v = chunk.split("=", 1)
        fields[k.strip()] = v.strip()

    cred = fields.get("Credential")
    signed = fields.get("SignedHeaders")
    sig = fields.get("Signature")
    if not (cred and signed and sig):
        raise AuthorizationHeaderMalformed("missing one of Credential/SignedHeaders/Signature")
    cred_parts = cred.split("/")
    if len(cred_parts) != 5:
        raise AuthorizationHeaderMalformed(f"malformed Credential field: {cred!r}")
    access_key, date, region, service, terminator = cred_parts
    if terminator != "aws4_request":
        raise AuthorizationHeaderMalformed(
            f"Credential terminator must be aws4_request, got {terminator!r}"
        )
    return AuthorizationParts(
        access_key=access_key,
        date=date,
        region=region,
        service=service,
        signed_headers=signed.split(";"),
        signature=sig,
        credential_scope=f"{date}/{region}/{service}/aws4_request",
    )


def parse_amz_date(value: str) -> datetime:
    """Parse a `x-amz-date` ISO8601 basic-format timestamp."""
    try:
        return datetime.strptime(value, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
    except ValueError as exc:
        raise AuthorizationHeaderMalformed(f"bad x-amz-date {value!r}") from exc


def check_clock_skew(
    amz_date: datetime, *, now: datetime | None = None, skew: int = DEFAULT_SKEW_SECONDS
) -> None:
    now = now or datetime.now(tz=UTC)
    delta = abs((now - amz_date).total_seconds())
    if delta > skew:
        raise RequestTimeTooSkewed(
            f"request time {amz_date.isoformat()} is {delta:.0f}s from server time"
        )


# ----------------------------------------------------------------------
# Canonical request construction
# ----------------------------------------------------------------------


def canonical_uri(path: str) -> str:
    """Compute the canonical URI per the SigV4 spec.

    For S3 we use the un-normalized path, with each segment URI-encoded
    once but slashes preserved.
    """
    if not path:
        return "/"
    # AWS spec: encode each segment with RFC 3986 unreserved set, then
    # join with `/`. We DO NOT collapse double slashes for S3 (S3 allows
    # them in object keys).
    parts = path.split("/")
    encoded = [quote(unquote(p), safe="-_.~") for p in parts]
    return "/".join(encoded) or "/"


def canonical_query_string(query: str) -> str:
    """Build the canonical query string for an S3 SigV4 request.

    S3's variant of SigV4 (`S3SigV4Auth` in botocore) does NOT re-encode
    query values — it preserves whatever the client sent on the wire and
    only sorts pairs by key (with value as tiebreaker). We must do the
    same to match the client's signature.
    """
    if not query:
        return ""
    pairs: list[tuple[str, str]] = []
    for kv in query.split("&"):
        if not kv:
            continue
        if "=" in kv:
            k, v = kv.split("=", 1)
        else:
            k, v = kv, ""
        pairs.append((k, v))
    pairs.sort()
    return "&".join(f"{k}={v}" for k, v in pairs)


def canonical_headers(headers: dict[str, str], signed_headers: list[str]) -> tuple[str, str]:
    """Build the CanonicalHeaders block and the SignedHeaders list."""
    lower_to_value: dict[str, str] = {k.lower(): v for k, v in headers.items()}
    lines: list[str] = []
    for h in signed_headers:
        value = lower_to_value.get(h, "")
        # Trim leading/trailing whitespace and collapse internal runs.
        cleaned = " ".join(value.strip().split())
        lines.append(f"{h}:{cleaned}\n")
    return "".join(lines), ";".join(signed_headers)


def build_canonical_request(
    *,
    method: str,
    path: str,
    query: str,
    headers: dict[str, str],
    signed_headers: list[str],
    payload_hash: str,
) -> str:
    canon_headers, signed_str = canonical_headers(headers, signed_headers)
    return "\n".join(
        [
            method.upper(),
            canonical_uri(path),
            canonical_query_string(query),
            canon_headers,
            signed_str,
            payload_hash,
        ]
    )


def build_string_to_sign(
    *,
    amz_date: str,
    credential_scope: str,
    canonical_request: str,
) -> str:
    return "\n".join(
        [
            ALGORITHM,
            amz_date,
            credential_scope,
            hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
        ]
    )


def derive_signing_key(
    secret: str,
    *,
    date: str,
    region: str,
    service: str = SERVICE,
) -> bytes:
    k_date = _hmac(("AWS4" + secret).encode("utf-8"), date)
    k_region = _hmac(k_date, region)
    k_service = _hmac(k_region, service)
    k_signing = _hmac(k_service, "aws4_request")
    return k_signing


def _hmac(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def compute_signature(signing_key: bytes, string_to_sign: str) -> str:
    return hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()


# ----------------------------------------------------------------------
# Top-level verifier
# ----------------------------------------------------------------------


class VerifiedRequest(NamedTuple):
    access_key: str
    signing_key: bytes
    seed_signature: str
    region: str
    payload_hash: str
    is_streaming: bool


def verify_header_auth(
    *,
    method: str,
    path: str,
    query: str,
    headers: dict[str, str],
    secret_lookup,
    now: datetime | None = None,
    skew: int = DEFAULT_SKEW_SECONDS,
) -> VerifiedRequest:
    """Verify a header-form SigV4 request.

    `headers` should be the raw header dict (case-insensitive lookup is done
    internally). `secret_lookup` is a callable taking an access key and
    returning the secret string or None.
    """
    headers_lower = {k.lower(): v for k, v in headers.items()}

    auth = headers_lower.get("authorization")
    if not auth:
        raise MissingAuthenticationToken("missing Authorization header")

    parts = parse_authorization_header(auth)

    # Per the AWS spec, `host` MUST always be in SignedHeaders. Without
    # it, an attacker who captured a signed request could replay it
    # against any nanio instance regardless of host (security audit
    # finding M1).
    if "host" not in parts.signed_headers:
        raise AuthorizationHeaderMalformed("SignedHeaders must include host")

    amz_date_str = headers_lower.get("x-amz-date") or headers_lower.get("date")
    if not amz_date_str:
        raise AuthorizationHeaderMalformed("missing x-amz-date header")
    amz_date = parse_amz_date(amz_date_str)
    check_clock_skew(amz_date, now=now, skew=skew)

    secret = secret_lookup(parts.access_key)
    if secret is None:
        # Use the default generic message — never reveal whether the
        # access key existed (security audit finding M2).
        raise InvalidAccessKeyId()

    payload_hash = headers_lower.get("x-amz-content-sha256", EMPTY_SHA256)

    canonical = build_canonical_request(
        method=method,
        path=path,
        query=query,
        headers=headers_lower,
        signed_headers=parts.signed_headers,
        payload_hash=payload_hash,
    )
    string_to_sign = build_string_to_sign(
        amz_date=amz_date_str,
        credential_scope=parts.credential_scope,
        canonical_request=canonical,
    )
    signing_key = derive_signing_key(
        secret, date=parts.date, region=parts.region, service=parts.service
    )
    expected = compute_signature(signing_key, string_to_sign)
    if not hmac.compare_digest(expected, parts.signature):
        raise SignatureDoesNotMatch(
            f"signature mismatch (got {parts.signature[:8]}…, expected {expected[:8]}…)"
        )

    return VerifiedRequest(
        access_key=parts.access_key,
        signing_key=signing_key,
        seed_signature=parts.signature,
        region=parts.region,
        payload_hash=payload_hash,
        is_streaming=payload_hash in (STREAMING_PAYLOAD, STREAMING_PAYLOAD_TRAILER),
    )


def verify_presigned_url(
    *,
    method: str,
    path: str,
    query: str,
    headers: dict[str, str],
    secret_lookup,
    now: datetime | None = None,
) -> VerifiedRequest:
    """Verify a presigned URL request (signature carried in the query string)."""
    qparams: dict[str, str] = {}
    for kv in query.split("&"):
        if not kv or "=" not in kv:
            continue
        k, v = kv.split("=", 1)
        qparams[k] = unquote(v)

    if qparams.get("X-Amz-Algorithm") != ALGORITHM:
        raise AuthorizationHeaderMalformed(
            f"expected X-Amz-Algorithm={ALGORITHM}, got {qparams.get('X-Amz-Algorithm')!r}"
        )

    cred = qparams.get("X-Amz-Credential")
    if not cred:
        raise AuthorizationHeaderMalformed("missing X-Amz-Credential")
    cred_parts = cred.split("/")
    if len(cred_parts) != 5:
        raise AuthorizationHeaderMalformed(f"malformed X-Amz-Credential: {cred!r}")
    access_key, date, region, service, _ = cred_parts

    amz_date_str = qparams.get("X-Amz-Date")
    if not amz_date_str:
        raise AuthorizationHeaderMalformed("missing X-Amz-Date")
    amz_date = parse_amz_date(amz_date_str)

    expires = int(qparams.get("X-Amz-Expires", "3600"))
    if expires < 1 or expires > 7 * 24 * 3600:
        raise AuthorizationHeaderMalformed(f"X-Amz-Expires out of range: {expires}")
    now = now or datetime.now(tz=UTC)
    if (now - amz_date).total_seconds() > expires:
        raise RequestTimeTooSkewed("presigned URL expired")
    # Allow modest clock skew the other way too.
    if amz_date - now > timedelta(seconds=DEFAULT_SKEW_SECONDS):
        raise RequestTimeTooSkewed("presigned URL is from the future")

    signed_headers = qparams.get("X-Amz-SignedHeaders", "host").split(";")
    # Refuse signed-headers lists that omit `host`, including the empty
    # case `X-Amz-SignedHeaders=` which splits to `[""]` (security audit
    # finding M1).
    if "host" not in signed_headers:
        raise AuthorizationHeaderMalformed("SignedHeaders must include host")
    given_signature = qparams.get("X-Amz-Signature")
    if not given_signature:
        raise AuthorizationHeaderMalformed("missing X-Amz-Signature")

    secret = secret_lookup(access_key)
    if secret is None:
        # Generic message — never reveal access-key existence (M2).
        raise InvalidAccessKeyId()

    # The canonical query string for presigned URLs excludes X-Amz-Signature.
    qparams_for_canon = {k: v for k, v in qparams.items() if k != "X-Amz-Signature"}
    canon_query = "&".join(
        f"{quote(k, safe='-_.~')}={quote(v, safe='-_.~')}"
        for k, v in sorted(qparams_for_canon.items())
    )

    headers_lower = {k.lower(): v for k, v in headers.items()}
    canonical = build_canonical_request(
        method=method,
        path=path,
        query=canon_query,
        headers=headers_lower,
        signed_headers=signed_headers,
        payload_hash=UNSIGNED_PAYLOAD,
    )
    string_to_sign = build_string_to_sign(
        amz_date=amz_date_str,
        credential_scope=f"{date}/{region}/{service}/aws4_request",
        canonical_request=canonical,
    )
    signing_key = derive_signing_key(secret, date=date, region=region, service=service)
    expected = compute_signature(signing_key, string_to_sign)
    if not hmac.compare_digest(expected, given_signature):
        raise SignatureDoesNotMatch(
            f"presigned signature mismatch (got {given_signature[:8]}…, expected {expected[:8]}…)"
        )

    return VerifiedRequest(
        access_key=access_key,
        signing_key=signing_key,
        seed_signature=given_signature,
        region=region,
        payload_hash=UNSIGNED_PAYLOAD,
        is_streaming=False,
    )
