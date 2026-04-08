"""Helpers for safely consuming request bodies in handlers.

The goal is twofold:

1. Never let a malicious client buffer an arbitrary amount of body data
   into the server process. The plain `await request.body()` from
   Starlette is unbounded, so we wrap it with a hard cap.

2. Never let a malicious XML body trigger entity-expansion (billion
   laughs) or quadratic blowup attacks. We parse all incoming XML
   through `defusedxml.ElementTree` instead of the stdlib `xml.etree`.

Both rules are golden-rule documented in `CLAUDE.md`. Every handler that
parses an XML request body MUST go through `read_bounded_body` and
`parse_xml_safely` from this module.
"""

from __future__ import annotations

from xml.etree import ElementTree as ET

from defusedxml import ElementTree as DefusedET
from defusedxml.common import (
    DTDForbidden,
    EntitiesForbidden,
    ExternalReferenceForbidden,
)
from starlette.requests import Request

from nanio.errors import EntityTooLarge, MalformedXML, MetadataTooLarge

DEFAULT_MAX_XML_BODY = 1 * 1024 * 1024
"""Upper bound on XML request bodies. 1 MiB fits the DeleteObjects 1000-key
cap and the CompleteMultipartUpload 10 000-part cap with generous headroom."""

USER_META_PREFIX = "x-amz-meta-"

MAX_USER_METADATA_BYTES = 2 * 1024
"""AWS S3's 2 KiB cap on the total size of user metadata on an object."""

# Exceptions defusedxml raises when it refuses a payload. We translate these
# to `MalformedXML`; anything else falls through to the catch-all handler
# in app.py so that unexpected bugs get logged with a full traceback instead
# of being silently re-badged as a client error.
_DEFUSED_REJECTIONS: tuple[type[Exception], ...] = (
    EntitiesForbidden,
    DTDForbidden,
    ExternalReferenceForbidden,
)


async def read_bounded_body(request: Request, *, max_bytes: int = DEFAULT_MAX_XML_BODY) -> bytes:
    """Read a request body into memory, refusing to exceed `max_bytes`.

    The cap is enforced BEFORE extending the buffer so peak memory usage
    stays at `max_bytes` rather than briefly overshooting by one chunk.
    """
    buf = bytearray()
    async for chunk in request.stream():
        if not chunk:
            continue
        if len(buf) + len(chunk) > max_bytes:
            raise EntityTooLarge(f"request body exceeds maximum of {max_bytes} bytes")
        buf.extend(chunk)
    return bytes(buf)


def parse_xml_safely(body: bytes) -> ET.Element:
    """Parse `body` as XML through `defusedxml`, rejecting entity-expansion
    and external-reference attacks.

    Only the documented defusedxml rejection exceptions and `ET.ParseError`
    are translated to `MalformedXML`. Anything else propagates so the
    catch-all handler in `app.py` can log a full traceback and return
    `InternalError`. Client wire behavior is unchanged; server-side
    debugging is preserved.

    `forbid_dtd=True` is defense in depth — S3 clients never send DTDs,
    and blocking them preemptively shrinks the attack surface.
    """
    if not body:
        raise MalformedXML("request body is empty")
    try:
        return DefusedET.fromstring(body, forbid_dtd=True)  # type: ignore[no-any-return]
    except ET.ParseError as exc:
        raise MalformedXML(f"request body is not valid XML: {exc}") from exc
    except _DEFUSED_REJECTIONS as exc:
        raise MalformedXML(f"request body rejected by XML parser: {exc}") from exc


def extract_user_metadata(
    request: Request, *, max_bytes: int = MAX_USER_METADATA_BYTES
) -> dict[str, str]:
    """Collect `x-amz-meta-*` headers, enforcing a byte-size cap.

    HTTP headers are latin-1 on the wire, so the cap counts encoded bytes
    rather than Python `str` character count — a UTF-8 emoji in a header
    value would otherwise only contribute 1 to `len()` while occupying 4
    bytes in the serialized request.
    """
    out: dict[str, str] = {}
    total_bytes = 0
    for raw_key, value in request.headers.items():
        lower = raw_key.lower()
        if not lower.startswith(USER_META_PREFIX):
            continue
        out[lower] = value
        total_bytes += len(lower.encode("latin-1", "replace")) + len(
            value.encode("latin-1", "replace")
        )
        if total_bytes > max_bytes:
            raise MetadataTooLarge(f"user metadata exceeds maximum of {max_bytes} bytes")
    return out
