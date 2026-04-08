from __future__ import annotations

from xml.etree import ElementTree as ET

import pytest

from nanio.auth.middleware import _send_error
from nanio.errors import InvalidAccessKeyId, SignatureDoesNotMatch


async def _render_error(exc):
    messages = []

    async def send(message):
        messages.append(message)

    await _send_error(send, exc)
    start = next(message for message in messages if message["type"] == "http.response.start")
    body = next(message["body"] for message in messages if message["type"] == "http.response.body")
    return start["status"], body


@pytest.mark.asyncio
async def test_send_error_preserves_aws_auth_error_code():
    invalid_status, invalid_body = await _render_error(InvalidAccessKeyId())
    mismatch_status, mismatch_body = await _render_error(SignatureDoesNotMatch())

    assert invalid_status == 403
    assert mismatch_status == 403
    invalid_root = ET.fromstring(invalid_body)
    mismatch_root = ET.fromstring(mismatch_body)

    assert invalid_root.findtext("Code") == "InvalidAccessKeyId"
    assert mismatch_root.findtext("Code") == "SignatureDoesNotMatch"
