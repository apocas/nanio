"""Bucket-level handlers.

A request like `GET /widgets` could be one of many things depending on
its query string:

- `GET /widgets`                 -> ListObjects (V1, deprecated but still used)
- `GET /widgets?list-type=2`     -> ListObjectsV2
- `GET /widgets?location`        -> GetBucketLocation
- `GET /widgets?uploads`         -> ListMultipartUploads (step 9)
- `PUT /widgets`                 -> CreateBucket
- `DELETE /widgets`              -> DeleteBucket
- `HEAD /widgets`                -> HeadBucket
- `POST /widgets?delete`         -> DeleteObjects batch (step 10)

The dispatcher reads method + query string and routes to the right
worker function. Each worker is small and testable in isolation.
"""

from __future__ import annotations

from starlette.requests import Request
from starlette.responses import Response

from nanio.app_state import get_settings, get_storage
from nanio.errors import MalformedXML, S3Error
from nanio.handlers._body import parse_xml_safely, read_bounded_body
from nanio.handlers.multipart import list_multipart_uploads_for_bucket
from nanio.xml import (
    delete_result_xml,
    list_objects_v2_xml,
    location_constraint_xml,
)

# AWS S3 caps DeleteObjects at 1000 keys per request.
MAX_DELETE_KEYS = 1000


async def dispatch_bucket(request: Request) -> Response:
    bucket = request.path_params["bucket"]
    method = request.method
    qp = request.query_params

    if method == "PUT":
        return await create_bucket(request, bucket)
    if method == "DELETE":
        return await delete_bucket(request, bucket)
    if method == "HEAD":
        return await head_bucket(request, bucket)
    if method == "GET":
        if "location" in qp:
            return await get_bucket_location(request, bucket)
        if "uploads" in qp:
            return await list_multipart_uploads_for_bucket(request, bucket)
        return await list_objects(request, bucket)
    if method == "POST":
        if "delete" in qp:
            return await delete_objects(request, bucket)
        return Response(status_code=405)
    return Response(status_code=405)


# ---------------------------------------------------------------------------


async def create_bucket(request: Request, bucket: str) -> Response:
    storage = get_storage(request)
    storage.create_bucket(bucket)
    return Response(
        status_code=200,
        headers={"Location": f"/{bucket}"},
    )


async def delete_bucket(request: Request, bucket: str) -> Response:
    storage = get_storage(request)
    storage.delete_bucket(bucket)
    return Response(status_code=204)


async def head_bucket(request: Request, bucket: str) -> Response:
    storage = get_storage(request)
    storage.head_bucket(bucket)
    return Response(status_code=200)


async def get_bucket_location(request: Request, bucket: str) -> Response:
    storage = get_storage(request)
    settings = get_settings(request)
    storage.head_bucket(bucket)  # 404 if missing
    body = location_constraint_xml(settings.region)
    return Response(content=body, media_type="application/xml", status_code=200)


async def delete_objects(request: Request, bucket: str) -> Response:
    storage = get_storage(request)
    body = await read_bounded_body(request)
    root = parse_xml_safely(body)

    keys: list[str] = []
    for elem in root:
        tag = elem.tag.split("}", 1)[-1]
        if tag != "Object":
            continue
        key_el = next((c for c in elem if c.tag.split("}", 1)[-1] == "Key"), None)
        if key_el is None or key_el.text is None:
            raise MalformedXML("Object missing Key")
        keys.append(key_el.text)
        if len(keys) > MAX_DELETE_KEYS:
            raise MalformedXML(f"DeleteObjects supports at most {MAX_DELETE_KEYS} keys per request")

    deleted: list[str] = []
    errors: list[tuple[str, str, str]] = []
    for key in keys:
        try:
            storage.delete_object(bucket, key)
            deleted.append(key)
        except S3Error as exc:
            errors.append((key, exc.code, exc.message_text))

    body_xml = delete_result_xml(deleted=deleted, errors=errors)
    return Response(content=body_xml, media_type="application/xml", status_code=200)


async def list_objects(request: Request, bucket: str) -> Response:
    storage = get_storage(request)
    settings = get_settings(request)
    qp = request.query_params

    prefix = qp.get("prefix", "")
    delimiter = qp.get("delimiter")
    encoding_type = qp.get("encoding-type")
    start_after = qp.get("start-after")
    continuation_token = qp.get("continuation-token")
    try:
        max_keys = int(qp.get("max-keys", str(settings.max_list_keys)))
    except ValueError:
        max_keys = settings.max_list_keys
    max_keys = max(0, min(max_keys, settings.max_list_keys))

    result = storage.list_objects(
        bucket,
        prefix=prefix,
        delimiter=delimiter,
        max_keys=max_keys,
        continuation_token=continuation_token,
        start_after=start_after,
    )

    body = list_objects_v2_xml(
        bucket=bucket,
        contents=[(c.key, c.last_modified, c.etag, c.size) for c in result.contents],
        common_prefixes=result.common_prefixes,
        prefix=prefix,
        delimiter=delimiter,
        max_keys=max_keys,
        is_truncated=result.is_truncated,
        continuation_token=continuation_token,
        next_continuation_token=result.next_continuation_token,
        start_after=start_after,
        encoding_type=encoding_type,
    )
    return Response(content=body, media_type="application/xml", status_code=200)
