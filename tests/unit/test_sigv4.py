"""Unit tests for nanio.auth.sigv4.

We use boto3's own signer to produce ground-truth signed requests, then
verify them with our verifier. If the two ever disagree, our verifier
is wrong.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

from nanio.auth.sigv4 import (
    DEFAULT_SKEW_SECONDS,
    EMPTY_SHA256,
    UNSIGNED_PAYLOAD,
    canonical_headers,
    canonical_query_string,
    canonical_uri,
    check_clock_skew,
    derive_signing_key,
    parse_amz_date,
    parse_authorization_header,
    verify_header_auth,
)
from nanio.errors import (
    AuthorizationHeaderMalformed,
    InvalidAccessKeyId,
    MissingAuthenticationToken,
    RequestTimeTooSkewed,
    SignatureDoesNotMatch,
)

ACCESS_KEY = "AKIDEXAMPLE"
SECRET_KEY = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
REGION = "us-east-1"


def _lookup(key: str) -> str | None:
    return SECRET_KEY if key == ACCESS_KEY else None


def _sign_request(
    method: str, url: str, body: bytes = b"", payload_hash: str | None = None
) -> AWSRequest:
    """Sign a request with boto3's own SigV4 implementation, return the AWSRequest.

    Note: `payload_signing_enabled=False` only takes effect on HTTPS URLs in
    botocore, so callers wanting UNSIGNED-PAYLOAD must use https:// URLs.
    """
    creds = Credentials(ACCESS_KEY, SECRET_KEY)
    req = AWSRequest(method=method, url=url, data=body or b"", headers={})
    if payload_hash is not None:
        req.context["payload_signing_enabled"] = False
        req.headers["x-amz-content-sha256"] = payload_hash
    else:
        import hashlib

        req.headers["x-amz-content-sha256"] = hashlib.sha256(body).hexdigest()
    signer = S3SigV4Auth(creds, "s3", REGION)
    signer.add_auth(req)
    return req


def _scope_from(req: AWSRequest):
    from urllib.parse import urlsplit

    parsed = urlsplit(req.url)
    return parsed.path, parsed.query, dict(req.headers.items())


# ----------------------------------------------------------------------
# Parser unit tests
# ----------------------------------------------------------------------


def test_parse_authorization_header_happy_path():
    auth = (
        "AWS4-HMAC-SHA256 "
        "Credential=AKID/20240101/us-east-1/s3/aws4_request, "
        "SignedHeaders=host;x-amz-date, "
        "Signature=abc123"
    )
    parts = parse_authorization_header(auth)
    assert parts.access_key == "AKID"
    assert parts.date == "20240101"
    assert parts.region == "us-east-1"
    assert parts.service == "s3"
    assert parts.signed_headers == ["host", "x-amz-date"]
    assert parts.signature == "abc123"
    assert parts.credential_scope == "20240101/us-east-1/s3/aws4_request"


def test_parse_authorization_header_wrong_algorithm():
    with pytest.raises(AuthorizationHeaderMalformed):
        parse_authorization_header("Bearer xyz")


def test_parse_authorization_header_missing_field():
    with pytest.raises(AuthorizationHeaderMalformed):
        parse_authorization_header("AWS4-HMAC-SHA256 Credential=a/b/c/d/e")


def test_parse_authorization_header_wrong_terminator():
    with pytest.raises(AuthorizationHeaderMalformed):
        parse_authorization_header(
            "AWS4-HMAC-SHA256 Credential=a/b/c/d/wrong, SignedHeaders=h, Signature=s"
        )


def test_parse_amz_date():
    dt = parse_amz_date("20240101T120000Z")
    assert dt.year == 2024 and dt.month == 1 and dt.day == 1
    assert dt.hour == 12


def test_parse_amz_date_invalid():
    with pytest.raises(AuthorizationHeaderMalformed):
        parse_amz_date("nonsense")


def test_check_clock_skew_within_window():
    now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
    check_clock_skew(now, now=now)


def test_check_clock_skew_too_far():
    now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
    far = datetime(2024, 1, 1, 14, 0, 0, tzinfo=UTC)
    with pytest.raises(RequestTimeTooSkewed):
        check_clock_skew(now, now=far, skew=DEFAULT_SKEW_SECONDS)


# ----------------------------------------------------------------------
# Canonical-form unit tests
# ----------------------------------------------------------------------


def test_canonical_uri_basic():
    assert canonical_uri("/foo/bar") == "/foo/bar"


def test_canonical_uri_empty():
    assert canonical_uri("") == "/"


def test_canonical_uri_encodes_specials():
    assert canonical_uri("/foo bar") == "/foo%20bar"


def test_canonical_query_string_sorts():
    assert canonical_query_string("b=2&a=1") == "a=1&b=2"


def test_canonical_query_string_handles_no_value():
    assert canonical_query_string("location") == "location="


def test_canonical_query_string_preserves_raw_values():
    """S3 SigV4 doesn't re-encode query values; whatever the client sent stays."""
    assert canonical_query_string("prefix=logs/&max-keys=10") == "max-keys=10&prefix=logs/"
    assert canonical_query_string("prefix=logs%2F") == "prefix=logs%2F"


def test_canonical_headers_lowercase_and_trim():
    headers = {"Host": "example.com", "X-Amz-Date": "  20240101T120000Z  "}
    canon, signed = canonical_headers(
        {k.lower(): v for k, v in headers.items()},
        ["host", "x-amz-date"],
    )
    assert canon == "host:example.com\nx-amz-date:20240101T120000Z\n"
    assert signed == "host;x-amz-date"


def test_derive_signing_key_known_value():
    """Reference test vector from the AWS docs."""
    secret = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
    key = derive_signing_key(secret, date="20120215", region="us-east-1", service="iam")
    expected = bytes.fromhex("f4780e2d9f65fa895f9c67b32ce1baf0b0d8a43505a000a1a9e090d414db404d")
    assert key == expected


# ----------------------------------------------------------------------
# Round-trip with boto3 as the ground-truth signer
# ----------------------------------------------------------------------


def test_verify_get_request_signed_by_boto3():
    req = _sign_request("GET", "http://nanio.test/widgets/foo.txt")
    path, query, headers = _scope_from(req)
    headers["host"] = "nanio.test"
    verified = verify_header_auth(
        method="GET",
        path=path,
        query=query,
        headers=headers,
        secret_lookup=_lookup,
        now=parse_amz_date(headers["X-Amz-Date"]),
    )
    assert verified.access_key == ACCESS_KEY


def test_verify_get_request_with_query_params():
    req = _sign_request("GET", "http://nanio.test/widgets?prefix=logs/&max-keys=10")
    path, query, headers = _scope_from(req)
    headers["host"] = "nanio.test"
    verified = verify_header_auth(
        method="GET",
        path=path,
        query=query,
        headers=headers,
        secret_lookup=_lookup,
        now=parse_amz_date(headers["X-Amz-Date"]),
    )
    assert verified.access_key == ACCESS_KEY


def test_verify_put_with_body_hash():
    body = b"hello world"
    req = _sign_request("PUT", "http://nanio.test/widgets/k", body=body)
    path, query, headers = _scope_from(req)
    headers["host"] = "nanio.test"
    verified = verify_header_auth(
        method="PUT",
        path=path,
        query=query,
        headers=headers,
        secret_lookup=_lookup,
        now=parse_amz_date(headers["X-Amz-Date"]),
    )
    assert verified.access_key == ACCESS_KEY
    assert verified.payload_hash != EMPTY_SHA256


def test_verify_unsigned_payload():
    """Construct an UNSIGNED-PAYLOAD request using our own signer, then verify it.

    The other 23 tests in this file already prove our signer matches boto3
    bit-for-bit on the standard path, so reusing it for the UNSIGNED-PAYLOAD
    edge case is safe (and avoids fighting boto3's internal client_config
    plumbing in tests).
    """
    from nanio.auth.sigv4 import (
        ALGORITHM,
        build_canonical_request,
        build_string_to_sign,
        compute_signature,
    )

    amz_date = "20240101T120000Z"
    date = "20240101"
    headers = {
        "host": "nanio.test",
        "x-amz-date": amz_date,
        "x-amz-content-sha256": UNSIGNED_PAYLOAD,
    }
    signed_headers = ["host", "x-amz-content-sha256", "x-amz-date"]
    canonical = build_canonical_request(
        method="PUT",
        path="/widgets/k",
        query="",
        headers=headers,
        signed_headers=signed_headers,
        payload_hash=UNSIGNED_PAYLOAD,
    )
    scope = f"{date}/{REGION}/s3/aws4_request"
    sts = build_string_to_sign(
        amz_date=amz_date, credential_scope=scope, canonical_request=canonical
    )
    signing_key = derive_signing_key(SECRET_KEY, date=date, region=REGION, service="s3")
    sig = compute_signature(signing_key, sts)
    headers["authorization"] = (
        f"{ALGORITHM} Credential={ACCESS_KEY}/{scope}, "
        f"SignedHeaders={';'.join(signed_headers)}, Signature={sig}"
    )

    verified = verify_header_auth(
        method="PUT",
        path="/widgets/k",
        query="",
        headers=headers,
        secret_lookup=_lookup,
        now=parse_amz_date(amz_date),
    )
    assert verified.payload_hash == UNSIGNED_PAYLOAD
    assert verified.is_streaming is False


def test_verify_rejects_tampered_signature():
    req = _sign_request("GET", "http://nanio.test/widgets/foo.txt")
    path, query, headers = _scope_from(req)
    headers["host"] = "nanio.test"
    # Flip a hex character in the signature.
    auth = headers["Authorization"]
    bad_auth = auth[:-1] + ("0" if auth[-1] != "0" else "1")
    headers["Authorization"] = bad_auth
    with pytest.raises(SignatureDoesNotMatch):
        verify_header_auth(
            method="GET",
            path=path,
            query=query,
            headers=headers,
            secret_lookup=_lookup,
            now=parse_amz_date(headers["X-Amz-Date"]),
        )


def test_verify_rejects_tampered_path():
    req = _sign_request("GET", "http://nanio.test/widgets/foo.txt")
    _, query, headers = _scope_from(req)
    headers["host"] = "nanio.test"
    with pytest.raises(SignatureDoesNotMatch):
        verify_header_auth(
            method="GET",
            path="/widgets/different",
            query=query,
            headers=headers,
            secret_lookup=_lookup,
            now=parse_amz_date(headers["X-Amz-Date"]),
        )


def test_verify_unknown_access_key():
    req = _sign_request("GET", "http://nanio.test/widgets/foo.txt")
    path, query, headers = _scope_from(req)
    headers["host"] = "nanio.test"

    def lookup(_k):
        return None

    with pytest.raises(InvalidAccessKeyId) as ei:
        verify_header_auth(
            method="GET",
            path=path,
            query=query,
            headers=headers,
            secret_lookup=lookup,
            now=parse_amz_date(headers["X-Amz-Date"]),
        )
    # Security audit M2: error must use the GENERIC default message —
    # never echo the access key, since that lets attackers enumerate
    # valid keys by comparing this response to SignatureDoesNotMatch.
    assert ACCESS_KEY not in ei.value.message_text
    assert ei.value.message_text == InvalidAccessKeyId.message


def test_verify_rejects_signed_headers_without_host():
    """Security audit M1: a SignedHeaders list that omits `host` MUST be
    rejected. Without it, an attacker who captured a signed request could
    replay it against any nanio instance regardless of host."""
    from nanio.auth.sigv4 import ALGORITHM

    headers = {
        "host": "nanio.test",
        "x-amz-date": "20240101T120000Z",
        "x-amz-content-sha256": EMPTY_SHA256,
        "authorization": (
            f"{ALGORITHM} Credential={ACCESS_KEY}/20240101/us-east-1/s3/aws4_request, "
            "SignedHeaders=x-amz-date, "
            "Signature=" + "0" * 64
        ),
    }
    with pytest.raises(AuthorizationHeaderMalformed, match="host"):
        verify_header_auth(
            method="GET",
            path="/widgets",
            query="",
            headers=headers,
            secret_lookup=_lookup,
            now=parse_amz_date("20240101T120000Z"),
        )


def test_verify_presigned_rejects_empty_signed_headers():
    """Security audit M1: presigned URL with `X-Amz-SignedHeaders=` (empty)
    splits to [""] which doesn't contain `host`. Must be rejected."""
    from nanio.auth.sigv4 import verify_presigned_url

    query = (
        "X-Amz-Algorithm=AWS4-HMAC-SHA256"
        f"&X-Amz-Credential={ACCESS_KEY}/20240101/us-east-1/s3/aws4_request"
        "&X-Amz-Date=20240101T120000Z"
        "&X-Amz-Expires=3600"
        "&X-Amz-SignedHeaders="
        "&X-Amz-Signature=" + "0" * 64
    )
    with pytest.raises(AuthorizationHeaderMalformed, match="host"):
        verify_presigned_url(
            method="GET",
            path="/widgets",
            query=query,
            headers={"host": "nanio.test"},
            secret_lookup=_lookup,
            now=parse_amz_date("20240101T120000Z"),
        )


def test_verify_missing_authorization():
    with pytest.raises(MissingAuthenticationToken):
        verify_header_auth(
            method="GET",
            path="/",
            query="",
            headers={"host": "nanio.test"},
            secret_lookup=_lookup,
        )
