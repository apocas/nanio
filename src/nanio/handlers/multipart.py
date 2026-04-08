"""HTTP handlers for the multipart upload endpoints.

Routing:
- POST /<bucket>/<key>?uploads          → create multipart upload
- PUT  /<bucket>/<key>?partNumber=N&uploadId=u → upload part
- POST /<bucket>/<key>?uploadId=u       → complete multipart upload
- DELETE /<bucket>/<key>?uploadId=u     → abort multipart upload
- GET /<bucket>/<key>?uploadId=u        → list parts
- GET /<bucket>?uploads                  → list multipart uploads (handler in bucket.py)

The actual state lives in `nanio.storage.multipart.MultipartManager`,
which is constructed against the same `data_dir` as the storage backend.
"""

from __future__ import annotations

from starlette.requests import Request
from starlette.responses import Response

from nanio.app_state import get_settings
from nanio.errors import (
    InvalidArgument,
    MalformedXML,
    NoSuchBucket,
)
from nanio.handlers._body import parse_xml_safely, read_bounded_body
from nanio.storage.multipart import (
    MultipartInit,
    MultipartManager,
)
from nanio.storage.paths import bucket_dir
from nanio.xml import (
    complete_multipart_upload_xml,
    initiate_multipart_upload_xml,
    list_multipart_uploads_xml,
    list_parts_xml,
)

USER_META_PREFIX = "x-amz-meta-"

# AWS S3 caps CompleteMultipartUpload at 10 000 parts.
MAX_COMPLETE_PARTS = 10_000

# AWS S3 caps user metadata at 2 KiB total. Mirror that here so the cap
# applies to multipart Create the same way it does to plain PUT (security
# audit finding M5).
MAX_USER_METADATA_BYTES = 2 * 1024


def _manager(request: Request) -> MultipartManager:
    settings = get_settings(request)
    return MultipartManager(settings.data_dir, chunk_size=settings.chunk_size)


def _extract_user_metadata(request: Request) -> dict[str, str]:
    out: dict[str, str] = {}
    total_bytes = 0
    for raw_key, value in request.headers.items():
        lower = raw_key.lower()
        if not lower.startswith(USER_META_PREFIX):
            continue
        out[lower] = value
        total_bytes += len(lower) + len(value)
        if total_bytes > MAX_USER_METADATA_BYTES:
            from nanio.errors import MetadataTooLarge

            raise MetadataTooLarge(
                f"user metadata exceeds maximum of {MAX_USER_METADATA_BYTES} bytes"
            )
    return out


# ---------------------------------------------------------------------------
# Create
# ---------------------------------------------------------------------------


async def create_multipart_upload(request: Request, bucket: str, key: str) -> Response:
    settings = get_settings(request)
    if not bucket_dir(settings.data_dir, bucket).is_dir():
        raise NoSuchBucket(resource=bucket)

    init = MultipartInit(
        bucket=bucket,
        key=key,
        content_type=request.headers.get("content-type", "application/octet-stream"),
        user_metadata=_extract_user_metadata(request),
        content_encoding=request.headers.get("content-encoding"),
        content_disposition=request.headers.get("content-disposition"),
        cache_control=request.headers.get("cache-control"),
    )
    upload_id = _manager(request).create(init)
    body = initiate_multipart_upload_xml(bucket=bucket, key=key, upload_id=upload_id)
    return Response(content=body, media_type="application/xml", status_code=200)


# ---------------------------------------------------------------------------
# Upload part
# ---------------------------------------------------------------------------


async def upload_part(request: Request, bucket: str, key: str) -> Response:
    settings = get_settings(request)
    if not bucket_dir(settings.data_dir, bucket).is_dir():
        raise NoSuchBucket(resource=bucket)

    upload_id = request.query_params.get("uploadId")
    pn_str = request.query_params.get("partNumber")
    if not upload_id or not pn_str:
        raise InvalidArgument("UploadPart requires uploadId and partNumber")
    try:
        part_number = int(pn_str)
    except ValueError as exc:
        raise InvalidArgument(f"invalid partNumber: {pn_str!r}") from exc

    manager = _manager(request)
    info = await manager.upload_part(upload_id, part_number, request.stream())
    return Response(status_code=200, headers={"ETag": info.etag})


# ---------------------------------------------------------------------------
# Complete
# ---------------------------------------------------------------------------


async def complete_multipart_upload(request: Request, bucket: str, key: str) -> Response:
    upload_id = request.query_params.get("uploadId")
    if not upload_id:
        raise InvalidArgument("CompleteMultipartUpload requires uploadId")
    body = await read_bounded_body(request)
    parts = _parse_complete_body(body)

    manager = _manager(request)
    info = manager.complete(upload_id, parts)
    location = f"http://{request.url.netloc}/{bucket}/{key}"
    response_xml = complete_multipart_upload_xml(
        bucket=bucket, key=key, etag=info.etag, location=location
    )
    return Response(content=response_xml, media_type="application/xml", status_code=200)


def _parse_complete_body(body: bytes) -> list[tuple[int, str]]:
    root = parse_xml_safely(body)
    # Element name may or may not have the S3 namespace.
    parts: list[tuple[int, str]] = []
    for elem in root:
        tag = elem.tag.split("}", 1)[-1]
        if tag != "Part":
            continue
        pn_el = next((c for c in elem if c.tag.split("}", 1)[-1] == "PartNumber"), None)
        et_el = next((c for c in elem if c.tag.split("}", 1)[-1] == "ETag"), None)
        if pn_el is None or et_el is None or pn_el.text is None or et_el.text is None:
            raise MalformedXML("Part missing PartNumber or ETag")
        try:
            pn = int(pn_el.text.strip())
        except ValueError as exc:
            raise MalformedXML(f"bad PartNumber: {pn_el.text!r}") from exc
        parts.append((pn, et_el.text.strip()))
        if len(parts) > MAX_COMPLETE_PARTS:
            raise MalformedXML(
                f"CompleteMultipartUpload supports at most {MAX_COMPLETE_PARTS} parts"
            )
    return parts


# ---------------------------------------------------------------------------
# Abort / list parts
# ---------------------------------------------------------------------------


async def abort_multipart_upload(request: Request, bucket: str, key: str) -> Response:
    upload_id = request.query_params.get("uploadId")
    if not upload_id:
        raise InvalidArgument("AbortMultipartUpload requires uploadId")
    _manager(request).abort(upload_id)
    return Response(status_code=204)


async def list_parts(request: Request, bucket: str, key: str) -> Response:
    upload_id = request.query_params.get("uploadId")
    if not upload_id:
        raise InvalidArgument("ListParts requires uploadId")
    manager = _manager(request)
    parts = manager.list_parts(upload_id)
    body = list_parts_xml(
        bucket=bucket,
        key=key,
        upload_id=upload_id,
        parts_listed=[(p.part_number, p.last_modified, p.etag, p.size) for p in parts],
    )
    return Response(content=body, media_type="application/xml", status_code=200)


# ---------------------------------------------------------------------------
# List uploads (bucket-level GET ?uploads)
# ---------------------------------------------------------------------------


async def list_multipart_uploads_for_bucket(request: Request, bucket: str) -> Response:
    settings = get_settings(request)
    if not bucket_dir(settings.data_dir, bucket).is_dir():
        raise NoSuchBucket(resource=bucket)
    manager = _manager(request)
    all_uploads = [
        (init.key, upload_id, init.initiated)
        for upload_id, init in manager.list_uploads()
        if init.bucket == bucket
    ]
    body = list_multipart_uploads_xml(bucket=bucket, uploads=all_uploads)
    return Response(content=body, media_type="application/xml", status_code=200)
