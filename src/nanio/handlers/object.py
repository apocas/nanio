"""Object-level handlers (PUT/GET/HEAD/DELETE on /<bucket>/<key>)."""

from __future__ import annotations

import re
from collections.abc import AsyncIterator
from urllib.parse import unquote

from starlette.requests import Request
from starlette.responses import Response, StreamingResponse

from nanio.app_state import get_storage
from nanio.errors import InvalidArgument, InvalidRequest
from nanio.handlers._body import extract_user_metadata
from nanio.handlers.multipart import (
    abort_multipart_upload,
    complete_multipart_upload,
    create_multipart_upload,
    list_parts,
    upload_part,
)
from nanio.storage.backend import ObjectInfo
from nanio.xml import copy_object_result_xml

_RANGE_RE = re.compile(r"^bytes=(\d+)-(\d*)$")


async def dispatch_object(request: Request) -> Response:
    bucket = request.path_params["bucket"]
    key = request.path_params["key"]
    method = request.method
    qp = request.query_params

    if method == "POST":
        if "uploads" in qp:
            return await create_multipart_upload(request, bucket, key)
        if "uploadId" in qp:
            return await complete_multipart_upload(request, bucket, key)
        return Response(status_code=405)
    if method == "PUT":
        if "uploadId" in qp and "partNumber" in qp:
            return await upload_part(request, bucket, key)
        if any(k.lower() == "x-amz-copy-source" for k in request.headers):
            return await copy_object(request, bucket, key)
        return await put_object(request, bucket, key)
    if method == "GET":
        if "uploadId" in qp:
            return await list_parts(request, bucket, key)
        return await get_object(request, bucket, key)
    if method == "HEAD":
        return await head_object(request, bucket, key)
    if method == "DELETE":
        if "uploadId" in qp:
            return await abort_multipart_upload(request, bucket, key)
        return await delete_object(request, bucket, key)
    return Response(status_code=405)


# ---------------------------------------------------------------------------


def _info_to_headers(info: ObjectInfo) -> dict[str, str]:
    headers = {
        "Content-Length": str(info.size),
        "ETag": info.etag,
        "Last-Modified": info.last_modified.strftime("%a, %d %b %Y %H:%M:%S GMT"),
        "Content-Type": info.content_type,
        "Accept-Ranges": "bytes",
        "x-amz-storage-class": info.storage_class,
    }
    if info.content_encoding:
        headers["Content-Encoding"] = info.content_encoding
    if info.content_disposition:
        headers["Content-Disposition"] = info.content_disposition
    if info.cache_control:
        headers["Cache-Control"] = info.cache_control
    for k, v in info.user_metadata.items():
        headers[k] = v
    return headers


async def put_object(request: Request, bucket: str, key: str) -> Response:
    storage = get_storage(request)
    content_type = request.headers.get("content-type", "application/octet-stream")
    expected_md5 = request.headers.get("content-md5")
    user_meta = extract_user_metadata(request)
    content_encoding = request.headers.get("content-encoding")
    content_disposition = request.headers.get("content-disposition")
    cache_control = request.headers.get("cache-control")

    info = await storage.put_object(
        bucket,
        key,
        request.stream(),
        content_type=content_type,
        user_metadata=user_meta,
        content_encoding=content_encoding,
        content_disposition=content_disposition,
        cache_control=cache_control,
        expected_md5=expected_md5,
    )
    return Response(
        status_code=200,
        headers={"ETag": info.etag},
    )


async def head_object(request: Request, bucket: str, key: str) -> Response:
    storage = get_storage(request)
    info = storage.head_object(bucket, key)
    return Response(status_code=200, headers=_info_to_headers(info))


async def get_object(request: Request, bucket: str, key: str) -> Response:
    storage = get_storage(request)
    range_start, range_end = _parse_range_header(request.headers.get("range"))
    result = await storage.get_object(bucket, key, range_start=range_start, range_end=range_end)
    info = result.info
    headers = _info_to_headers(info)
    if result.range_start is not None and result.range_end is not None:
        served = result.range_end - result.range_start + 1
        headers["Content-Length"] = str(served)
        headers["Content-Range"] = f"bytes {result.range_start}-{result.range_end}/{info.size}"
        status = 206
    else:
        status = 200

    return StreamingResponse(
        content=_iter_body(result.body),
        status_code=status,
        headers=headers,
        media_type=info.content_type,
    )


async def delete_object(request: Request, bucket: str, key: str) -> Response:
    storage = get_storage(request)
    storage.delete_object(bucket, key)
    return Response(status_code=204)


async def copy_object(request: Request, dst_bucket: str, dst_key: str) -> Response:
    storage = get_storage(request)
    raw_source = request.headers.get("x-amz-copy-source")
    if not raw_source:
        raise InvalidRequest("x-amz-copy-source header missing")
    src_bucket, src_key = _parse_copy_source(raw_source)
    info = await storage.copy_object(src_bucket, src_key, dst_bucket, dst_key)
    body = copy_object_result_xml(etag=info.etag, last_modified=info.last_modified)
    return Response(
        content=body,
        media_type="application/xml",
        status_code=200,
        headers={"x-amz-copy-source-version-id": "null"},
    )


def _parse_copy_source(raw: str) -> tuple[str, str]:
    """Decode an x-amz-copy-source header value into (bucket, key).

    The value is "[/]<bucket>/<key>". URL-encoded characters are allowed.
    """
    src = unquote(raw)
    if src.startswith("/"):
        src = src[1:]
    if "/" not in src:
        raise InvalidRequest(f"x-amz-copy-source must be bucket/key, got {raw!r}")
    bucket, key = src.split("/", 1)
    if not bucket or not key:
        raise InvalidRequest(f"x-amz-copy-source must be non-empty, got {raw!r}")
    return bucket, key


# ---------------------------------------------------------------------------


def _parse_range_header(value: str | None) -> tuple[int | None, int | None]:
    if not value:
        return None, None
    match = _RANGE_RE.match(value.strip())
    if not match:
        raise InvalidArgument(f"unsupported Range header: {value!r}")
    start = int(match.group(1))
    end_str = match.group(2)
    end = int(end_str) if end_str else None
    return start, end


async def _iter_body(body: AsyncIterator[bytes]) -> AsyncIterator[bytes]:
    async for chunk in body:
        yield chunk
