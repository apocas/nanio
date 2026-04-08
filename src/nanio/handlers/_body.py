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
from starlette.requests import Request

from nanio.errors import EntityTooLarge, MalformedXML

# 1 MiB is plenty for the bounded XML payloads we accept (DeleteObjects
# capped at 1000 keys, CompleteMultipartUpload capped at 10 000 parts).
# A 1 KiB headroom per entry is generous; tighter caps just push the work
# back to the batch-size validator after parsing.
DEFAULT_MAX_XML_BODY = 1 * 1024 * 1024


async def read_bounded_body(request: Request, *, max_bytes: int = DEFAULT_MAX_XML_BODY) -> bytes:
    """Read a request body into memory, refusing to exceed `max_bytes`.

    Raises `EntityTooLarge` as soon as the cap is exceeded — we never
    allocate a buffer larger than `max_bytes + chunk_size`.
    """
    buf = bytearray()
    async for chunk in request.stream():
        if not chunk:
            continue
        buf.extend(chunk)
        if len(buf) > max_bytes:
            raise EntityTooLarge(f"request body exceeds maximum of {max_bytes} bytes")
    return bytes(buf)


def parse_xml_safely(body: bytes) -> ET.Element:
    """Parse `body` as XML through `defusedxml`, rejecting entity-expansion attacks.

    On a parse error or any defusedxml security trigger, raises
    `MalformedXML`.
    """
    if not body:
        raise MalformedXML("request body is empty")
    try:
        return DefusedET.fromstring(body)  # type: ignore[no-any-return]
    except ET.ParseError as exc:
        raise MalformedXML(f"request body is not valid XML: {exc}") from exc
    except Exception as exc:  # defusedxml raises EntitiesForbidden, DTDForbidden, etc.
        raise MalformedXML(f"request body rejected by XML parser: {exc}") from exc
