"""Starlette middleware that enforces SigV4 on every request.

The middleware:

1. Reads the Authorization header (or X-Amz-* query params for presigned URLs).
2. Looks up the secret via the configured CredentialResolver.
3. Verifies the signature against the canonical request.
4. On success, stashes the verified principal on `request.state.principal`.
5. On failure, raises an `S3Error` subclass — the global exception handler
   serializes it to the standard XML body.

Test escape hatch: when `Settings.auth_disabled` is True, the middleware
becomes a no-op. This is ONLY used in unit/integration tests that don't
need to exercise auth.
"""

from __future__ import annotations

from starlette.requests import Request
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from nanio.auth.chunked import decode_aws_chunked
from nanio.auth.credentials import CredentialResolver
from nanio.auth.sigv4 import (
    VerifiedRequest,
    parse_authorization_header,
    verify_header_auth,
    verify_presigned_url,
)
from nanio.errors import (
    MissingAuthenticationToken,
    S3Error,
)


class SigV4Middleware:
    """Pure-ASGI middleware so we can wrap `receive` for chunked decoding later."""

    def __init__(
        self,
        app: ASGIApp,
        *,
        credentials: CredentialResolver,
        disabled: bool = False,
    ) -> None:
        self._app = app
        self._credentials = credentials
        self._disabled = disabled

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or self._disabled:
            await self._app(scope, receive, send)
            return

        request = Request(scope)
        try:
            verified = self._verify(request)
        except S3Error as exc:
            await _send_error(send, exc)
            return

        scope = dict(scope)
        scope["nanio.principal"] = verified.access_key

        if verified.is_streaming:
            # ASGI guarantees lowercase header names, so we only ever need
            # the lowercase keys here. (Security audit L1: dead-code
            # cleanup of an unreachable `Authorization` fallback.)
            headers = {k.decode("latin-1"): v.decode("latin-1") for k, v in scope["headers"]}
            amz_date = headers.get("x-amz-date") or headers.get("date", "")
            parts = parse_authorization_header(headers["authorization"])
            wrapped_receive = _make_chunked_receive(
                receive,
                signing_key=verified.signing_key,
                seed_signature=verified.seed_signature,
                amz_date=amz_date,
                scope_str=parts.credential_scope,
            )
            await self._app(scope, wrapped_receive, send)
            return

        await self._app(scope, receive, send)

    def _verify(self, request: Request) -> VerifiedRequest:
        headers = {k.decode("latin-1"): v.decode("latin-1") for k, v in request.scope["headers"]}
        method = request.method
        path = request.url.path
        query = request.url.query or ""

        if "X-Amz-Algorithm=" in query:
            return verify_presigned_url(
                method=method,
                path=path,
                query=query,
                headers=headers,
                secret_lookup=self._credentials.resolve,
            )

        if "authorization" not in {k.lower() for k in headers}:
            raise MissingAuthenticationToken("missing Authorization header")

        return verify_header_auth(
            method=method,
            path=path,
            query=query,
            headers=headers,
            secret_lookup=self._credentials.resolve,
        )


def _make_chunked_receive(
    receive: Receive,
    *,
    signing_key: bytes,
    seed_signature: str,
    amz_date: str,
    scope_str: str,
) -> Receive:
    """Wrap an ASGI receive callable so it yields decoded body chunks.

    The wrapped callable returns ASGI `http.request` messages whose body
    field contains the *decoded* payload, with `more_body` set so that
    streaming behavior downstream is preserved.
    """

    async def _source() -> bytes:
        msg = await receive()
        if msg["type"] != "http.request":  # pragma: no cover
            # Defensive: the ASGI spec guarantees this branch is only
            # reached for lifespan/disconnect messages, which don't
            # arrive on the HTTP body receive channel mid-request.
            return b""
        return bytes(msg.get("body") or b"")

    decoder = decode_aws_chunked(
        _source,
        signing_key=signing_key,
        seed_signature=seed_signature,
        amz_date=amz_date,
        scope=scope_str,
    ).__aiter__()

    done = False

    async def _wrapped() -> Message:
        nonlocal done
        if done:  # pragma: no cover
            # Defensive: Starlette only calls `receive` until `more_body`
            # is False, so this guard is belt-and-braces for handlers
            # that misbehave and re-pull the body after it's drained.
            return {"type": "http.request", "body": b"", "more_body": False}
        try:
            chunk = await decoder.__anext__()
        except StopAsyncIteration:
            done = True
            return {"type": "http.request", "body": b"", "more_body": False}
        return {"type": "http.request", "body": chunk, "more_body": True}

    return _wrapped


async def _send_error(send: Send, exc: S3Error) -> None:
    body = exc.to_xml()
    await send(
        {
            "type": "http.response.start",
            "status": exc.http_status,
            "headers": [
                (b"content-type", b"application/xml"),
                (b"content-length", str(len(body)).encode("ascii")),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})
