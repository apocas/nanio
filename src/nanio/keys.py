"""Bucket and object key validation, plus a safe path-join utility.

S3 has very specific rules for what counts as a valid bucket name and what
counts as a valid object key. We enforce them on the way in so that we never
have to think about edge cases (`..`, control characters, oversized keys)
deeper in the stack.

References:
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
"""

from __future__ import annotations

import os
import re
from pathlib import Path

from nanio.errors import InvalidBucketName, InvalidObjectName

# Bucket naming: 3-63 chars, lowercase letters, digits, hyphens, dots, must
# begin and end with a letter or digit, no consecutive dots, not an IP.
_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9\-.]{1,61}[a-z0-9]$")
_BUCKET_IP_RE = re.compile(r"^\d+\.\d+\.\d+\.\d+$")
_RESERVED_PREFIX = ".nanio"  # we use this for our metadata sidecar dir

# Object key rules:
#  - 1 to 1024 bytes (UTF-8)
#  - no NUL or other ASCII control characters
#  - we additionally forbid `..` path segments and absolute paths to keep
#    safe_join trivially correct.
_KEY_MAX_BYTES = 1024
_KEY_FORBIDDEN_CHARS = set(chr(c) for c in range(32)) | {chr(127)}


def validate_bucket_name(name: str) -> None:
    if not isinstance(name, str):
        raise InvalidBucketName(f"bucket name must be a string, got {type(name).__name__}")
    if len(name) < 3 or len(name) > 63:
        raise InvalidBucketName("bucket name must be 3-63 characters")
    if not _BUCKET_RE.match(name):
        raise InvalidBucketName(
            "bucket name must be lowercase letters/digits/hyphens/dots, "
            "starting and ending with a letter or digit"
        )
    if ".." in name:
        raise InvalidBucketName("bucket name may not contain consecutive dots")
    if _BUCKET_IP_RE.match(name):
        raise InvalidBucketName("bucket name may not be formatted as an IP address")
    if name.startswith(_RESERVED_PREFIX):
        raise InvalidBucketName(f"bucket name may not start with {_RESERVED_PREFIX!r}")


def validate_object_key(key: str) -> None:
    if not isinstance(key, str):
        raise InvalidObjectName(f"object key must be a string, got {type(key).__name__}")
    if not key:
        raise InvalidObjectName("object key may not be empty")
    encoded = key.encode("utf-8")
    if len(encoded) > _KEY_MAX_BYTES:
        raise InvalidObjectName(f"object key may not exceed {_KEY_MAX_BYTES} bytes")
    if any(c in _KEY_FORBIDDEN_CHARS for c in key):
        raise InvalidObjectName("object key contains forbidden control characters")
    # Forbid leading slashes and any segment that is `.` or `..`
    if key.startswith("/"):
        raise InvalidObjectName("object key may not start with `/`")
    for segment in key.split("/"):
        if segment in ("", ".", ".."):
            raise InvalidObjectName("object key may not contain `.`, `..`, or empty segments")
    # Reserved namespace for our own sidecar / multipart areas.
    if key.startswith(_RESERVED_PREFIX):
        raise InvalidObjectName(f"object key may not start with {_RESERVED_PREFIX!r}")


def safe_join(base: Path, *parts: str) -> Path:
    """Join `parts` onto `base`, refusing any escape outside `base`.

    `base` is expected to already be an absolute, resolved path. Each `parts`
    string is treated as a relative path segment. Raises ValueError if the
    final path would escape `base` (via `..`, symlinks resolving outside,
    or absolute components).
    """
    if not base.is_absolute():
        raise ValueError(f"safe_join base must be absolute, got {base}")
    candidate = base
    for p in parts:
        if os.path.isabs(p):
            raise ValueError(f"safe_join part must be relative, got absolute {p!r}")
        # Reject any `..` segment up front, even if normalization would
        # have kept us inside `base`. Defense in depth — clients should
        # never be able to use `..` to navigate the on-disk layout.
        for segment in p.replace("\\", "/").split("/"):
            if segment == "..":
                raise ValueError(f"safe_join refused `..` segment in {p!r}")
        candidate = candidate / p
    # Resolve without requiring the path to exist (Python 3.6+: strict=False).
    resolved = Path(os.path.normpath(candidate))
    try:
        resolved.relative_to(base)
    except ValueError as exc:
        raise ValueError(f"safe_join refused path escaping base: {resolved}") from exc
    return resolved
