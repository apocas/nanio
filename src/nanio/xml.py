"""XML response builders.

Pure functions, no I/O. We hand-roll the XML because the S3 dialect is small,
the namespace handling is simple, and `lxml`/`xmltodict` would add weight for
no real benefit.

Every builder returns `bytes` ready to send on the wire (UTF-8 encoded with
the standard XML declaration).

References:
- https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html
- https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
- https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import datetime
from urllib.parse import quote
from xml.sax.saxutils import escape as _xml_escape

S3_NS = "http://s3.amazonaws.com/doc/2006-03-01/"
_XML_DECL = '<?xml version="1.0" encoding="UTF-8"?>'


def _esc(value: str) -> str:
    return _xml_escape(value, {'"': "&quot;"})


def _iso(dt: datetime) -> str:
    """ISO 8601 in the exact format AWS uses for object timestamps."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _maybe_url_encode(key: str, encoding_type: str | None) -> str:
    if encoding_type == "url":
        return quote(key, safe="")
    return key


# ---------------------------------------------------------------------------
# Service / bucket / list ops
# ---------------------------------------------------------------------------


def list_buckets_xml(
    buckets: Iterable[tuple[str, datetime]],
    *,
    owner_id: str = "nanio",
    owner_display_name: str = "nanio",
) -> bytes:
    parts = [
        _XML_DECL,
        f'<ListAllMyBucketsResult xmlns="{S3_NS}">',
        "<Owner>",
        f"<ID>{_esc(owner_id)}</ID>",
        f"<DisplayName>{_esc(owner_display_name)}</DisplayName>",
        "</Owner>",
        "<Buckets>",
    ]
    for name, created in buckets:
        parts.extend(
            [
                "<Bucket>",
                f"<Name>{_esc(name)}</Name>",
                f"<CreationDate>{_iso(created)}</CreationDate>",
                "</Bucket>",
            ]
        )
    parts.append("</Buckets></ListAllMyBucketsResult>")
    return "".join(parts).encode("utf-8")


def list_objects_v2_xml(
    *,
    bucket: str,
    contents: Sequence[tuple[str, datetime, str, int]],  # (key, lastmod, etag, size)
    common_prefixes: Sequence[str],
    prefix: str = "",
    delimiter: str | None = None,
    max_keys: int = 1000,
    is_truncated: bool = False,
    continuation_token: str | None = None,
    next_continuation_token: str | None = None,
    start_after: str | None = None,
    encoding_type: str | None = None,
    storage_class: str = "STANDARD",
) -> bytes:
    parts = [
        _XML_DECL,
        f'<ListBucketResult xmlns="{S3_NS}">',
        f"<Name>{_esc(bucket)}</Name>",
        f"<Prefix>{_esc(_maybe_url_encode(prefix, encoding_type))}</Prefix>",
    ]
    if delimiter is not None:
        parts.append(f"<Delimiter>{_esc(_maybe_url_encode(delimiter, encoding_type))}</Delimiter>")
    parts.append(f"<MaxKeys>{max_keys}</MaxKeys>")
    if encoding_type:
        parts.append(f"<EncodingType>{_esc(encoding_type)}</EncodingType>")
    parts.append(f"<KeyCount>{len(contents) + len(common_prefixes)}</KeyCount>")
    parts.append(f"<IsTruncated>{'true' if is_truncated else 'false'}</IsTruncated>")
    if continuation_token is not None:
        parts.append(f"<ContinuationToken>{_esc(continuation_token)}</ContinuationToken>")
    if next_continuation_token is not None:
        parts.append(
            f"<NextContinuationToken>{_esc(next_continuation_token)}</NextContinuationToken>"
        )
    if start_after is not None:
        parts.append(f"<StartAfter>{_esc(_maybe_url_encode(start_after, encoding_type))}</StartAfter>")

    for key, last_mod, etag, size in contents:
        encoded_key = _maybe_url_encode(key, encoding_type)
        parts.extend(
            [
                "<Contents>",
                f"<Key>{_esc(encoded_key)}</Key>",
                f"<LastModified>{_iso(last_mod)}</LastModified>",
                f"<ETag>{_esc(etag)}</ETag>",
                f"<Size>{size}</Size>",
                f"<StorageClass>{_esc(storage_class)}</StorageClass>",
                "</Contents>",
            ]
        )

    for cp in common_prefixes:
        encoded_cp = _maybe_url_encode(cp, encoding_type)
        parts.extend(
            [
                "<CommonPrefixes>",
                f"<Prefix>{_esc(encoded_cp)}</Prefix>",
                "</CommonPrefixes>",
            ]
        )

    parts.append("</ListBucketResult>")
    return "".join(parts).encode("utf-8")


# ---------------------------------------------------------------------------
# Multipart upload ops
# ---------------------------------------------------------------------------


def initiate_multipart_upload_xml(*, bucket: str, key: str, upload_id: str) -> bytes:
    parts = [
        _XML_DECL,
        f'<InitiateMultipartUploadResult xmlns="{S3_NS}">',
        f"<Bucket>{_esc(bucket)}</Bucket>",
        f"<Key>{_esc(key)}</Key>",
        f"<UploadId>{_esc(upload_id)}</UploadId>",
        "</InitiateMultipartUploadResult>",
    ]
    return "".join(parts).encode("utf-8")


def complete_multipart_upload_xml(
    *,
    bucket: str,
    key: str,
    etag: str,
    location: str,
) -> bytes:
    parts = [
        _XML_DECL,
        f'<CompleteMultipartUploadResult xmlns="{S3_NS}">',
        f"<Location>{_esc(location)}</Location>",
        f"<Bucket>{_esc(bucket)}</Bucket>",
        f"<Key>{_esc(key)}</Key>",
        f"<ETag>{_esc(etag)}</ETag>",
        "</CompleteMultipartUploadResult>",
    ]
    return "".join(parts).encode("utf-8")


def list_parts_xml(
    *,
    bucket: str,
    key: str,
    upload_id: str,
    parts_listed: Sequence[tuple[int, datetime, str, int]],  # (part_number, lastmod, etag, size)
    max_parts: int = 1000,
    is_truncated: bool = False,
    next_part_marker: int | None = None,
    storage_class: str = "STANDARD",
) -> bytes:
    parts = [
        _XML_DECL,
        f'<ListPartsResult xmlns="{S3_NS}">',
        f"<Bucket>{_esc(bucket)}</Bucket>",
        f"<Key>{_esc(key)}</Key>",
        f"<UploadId>{_esc(upload_id)}</UploadId>",
        f"<StorageClass>{_esc(storage_class)}</StorageClass>",
        f"<MaxParts>{max_parts}</MaxParts>",
        f"<IsTruncated>{'true' if is_truncated else 'false'}</IsTruncated>",
    ]
    if next_part_marker is not None:
        parts.append(f"<NextPartNumberMarker>{next_part_marker}</NextPartNumberMarker>")
    for pn, lastmod, etag, size in parts_listed:
        parts.extend(
            [
                "<Part>",
                f"<PartNumber>{pn}</PartNumber>",
                f"<LastModified>{_iso(lastmod)}</LastModified>",
                f"<ETag>{_esc(etag)}</ETag>",
                f"<Size>{size}</Size>",
                "</Part>",
            ]
        )
    parts.append("</ListPartsResult>")
    return "".join(parts).encode("utf-8")


def list_multipart_uploads_xml(
    *,
    bucket: str,
    uploads: Sequence[tuple[str, str, datetime]],  # (key, upload_id, initiated)
    max_uploads: int = 1000,
    is_truncated: bool = False,
    storage_class: str = "STANDARD",
) -> bytes:
    parts = [
        _XML_DECL,
        f'<ListMultipartUploadsResult xmlns="{S3_NS}">',
        f"<Bucket>{_esc(bucket)}</Bucket>",
        f"<MaxUploads>{max_uploads}</MaxUploads>",
        f"<IsTruncated>{'true' if is_truncated else 'false'}</IsTruncated>",
    ]
    for key, upload_id, initiated in uploads:
        parts.extend(
            [
                "<Upload>",
                f"<Key>{_esc(key)}</Key>",
                f"<UploadId>{_esc(upload_id)}</UploadId>",
                f"<StorageClass>{_esc(storage_class)}</StorageClass>",
                f"<Initiated>{_iso(initiated)}</Initiated>",
                "</Upload>",
            ]
        )
    parts.append("</ListMultipartUploadsResult>")
    return "".join(parts).encode("utf-8")


# ---------------------------------------------------------------------------
# Delete batch + copy
# ---------------------------------------------------------------------------


def delete_result_xml(
    *,
    deleted: Sequence[str],
    errors: Sequence[tuple[str, str, str]],  # (key, code, message)
) -> bytes:
    parts = [_XML_DECL, f'<DeleteResult xmlns="{S3_NS}">']
    for key in deleted:
        parts.extend(["<Deleted>", f"<Key>{_esc(key)}</Key>", "</Deleted>"])
    for key, code, msg in errors:
        parts.extend(
            [
                "<Error>",
                f"<Key>{_esc(key)}</Key>",
                f"<Code>{_esc(code)}</Code>",
                f"<Message>{_esc(msg)}</Message>",
                "</Error>",
            ]
        )
    parts.append("</DeleteResult>")
    return "".join(parts).encode("utf-8")


def copy_object_result_xml(*, etag: str, last_modified: datetime) -> bytes:
    parts = [
        _XML_DECL,
        f'<CopyObjectResult xmlns="{S3_NS}">',
        f"<LastModified>{_iso(last_modified)}</LastModified>",
        f"<ETag>{_esc(etag)}</ETag>",
        "</CopyObjectResult>",
    ]
    return "".join(parts).encode("utf-8")


def location_constraint_xml(region: str) -> bytes:
    return (
        f'{_XML_DECL}<LocationConstraint xmlns="{S3_NS}">{_esc(region)}</LocationConstraint>'
    ).encode("utf-8")
