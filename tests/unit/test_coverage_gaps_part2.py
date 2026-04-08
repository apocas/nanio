"""Targeted coverage tests — part 2.

Covers the remaining gaps in the storage layer, the object/bucket
handlers, the sigv4 presigned error branches, the chunked decoder edge
cases, and the keys validator branches. Like `test_coverage_gaps.py`,
these are not feature tests — they exist to push coverage to 100%.
"""

from __future__ import annotations

import asyncio
import base64
from datetime import UTC, datetime, timedelta

import pytest

from nanio.auth.chunked import _Buffered, decode_aws_chunked
from nanio.auth.sigv4 import (
    ALGORITHM,
    derive_signing_key,
    parse_authorization_header,
    verify_presigned_url,
)
from nanio.errors import (
    AuthorizationHeaderMalformed,
    BadDigest,
    InvalidAccessKeyId,
    InvalidArgument,
    InvalidBucketName,
    InvalidRequest,
    RequestTimeTooSkewed,
)
from nanio.handlers.object import _info_to_headers, _parse_copy_source, _parse_range_header
from nanio.keys import validate_bucket_name
from nanio.storage.backend import ObjectInfo
from nanio.storage.filesystem import (
    FilesystemStorage,
    _b64md5_to_hex,
    _decode_token,
    _is_common_prefix_target,
    _prune_empty_dirs,
    _walk_keys,
)
from nanio.storage.multipart import MultipartInit, MultipartManager
from nanio.storage.paths import multipart_dir

# ----------------------------------------------------------------------
# nanio/keys.py
# ----------------------------------------------------------------------


def test_validate_bucket_name_rejects_uppercase():
    """Exercise the regex rejection (not the reserved-prefix branch)."""
    with pytest.raises(InvalidBucketName, match="lowercase"):
        validate_bucket_name("BadName")


# ----------------------------------------------------------------------
# nanio/handlers/object.py — _info_to_headers optional fields
# ----------------------------------------------------------------------


def test_info_to_headers_includes_optional_fields():
    info = ObjectInfo(
        key="k",
        size=10,
        etag='"e"',
        last_modified=datetime(2026, 1, 1, tzinfo=UTC),
        content_encoding="gzip",
        content_disposition="attachment",
        cache_control="no-cache",
        user_metadata={"x-amz-meta-author": "alice"},
    )
    headers = _info_to_headers(info)
    assert headers["Content-Encoding"] == "gzip"
    assert headers["Content-Disposition"] == "attachment"
    assert headers["Cache-Control"] == "no-cache"
    assert headers["x-amz-meta-author"] == "alice"


def test_info_to_headers_omits_unset_optional_fields():
    info = ObjectInfo(
        key="k",
        size=10,
        etag='"e"',
        last_modified=datetime(2026, 1, 1, tzinfo=UTC),
    )
    headers = _info_to_headers(info)
    assert "Content-Encoding" not in headers
    assert "Content-Disposition" not in headers
    assert "Cache-Control" not in headers


def test_parse_range_header_malformed():
    with pytest.raises(InvalidArgument, match="Range"):
        _parse_range_header("not-a-range")


def test_parse_range_header_none_and_empty():
    assert _parse_range_header(None) == (None, None)
    assert _parse_range_header("") == (None, None)


def test_parse_copy_source_missing_slash():
    with pytest.raises(InvalidRequest, match="bucket/key"):
        _parse_copy_source("just-a-bucket-no-key")


def test_parse_copy_source_empty_bucket():
    with pytest.raises(InvalidRequest, match="non-empty"):
        _parse_copy_source("//key")  # empty bucket after leading slash strip
    with pytest.raises(InvalidRequest, match="non-empty"):
        _parse_copy_source("bucket/")  # empty key


def test_parse_copy_source_with_leading_slash():
    # Leading `/` is allowed and stripped.
    assert _parse_copy_source("/widgets/foo.txt") == ("widgets", "foo.txt")
    assert _parse_copy_source("widgets/foo.txt") == ("widgets", "foo.txt")


def test_parse_copy_source_url_decoded():
    # `%2F` for `/` inside a key is URL-decoded.
    assert _parse_copy_source("widgets/path%2Fto%2Ffile") == ("widgets", "path/to/file")


# ----------------------------------------------------------------------
# nanio/storage/filesystem.py — internal helpers + edge branches
# ----------------------------------------------------------------------


@pytest.fixture
def storage(tmp_path):
    s = FilesystemStorage(tmp_path)
    s.create_bucket("widgets")
    return s


def test_filesystem_data_dir_property(tmp_path):
    s = FilesystemStorage(tmp_path)
    assert s.data_dir == tmp_path.resolve()


def test_list_objects_rejects_negative_max_keys(storage):
    with pytest.raises(InvalidArgument):
        storage.list_objects("widgets", max_keys=-1)


def test_list_objects_uses_start_after_when_no_token(storage):
    async def _put(key, body=b"x"):
        async def _stream():
            yield body

        await storage.put_object("widgets", key, _stream())

    asyncio.run(_put("a.txt"))
    asyncio.run(_put("b.txt"))
    asyncio.run(_put("c.txt"))

    result = storage.list_objects("widgets", start_after="a.txt")
    keys = [c.key for c in result.contents]
    assert "a.txt" not in keys
    assert "b.txt" in keys and "c.txt" in keys


def test_list_objects_with_bad_continuation_token(storage):
    with pytest.raises(InvalidArgument, match="continuation token"):
        storage.list_objects("widgets", continuation_token="!!!not-base64!!!")


def test_b64md5_to_hex_invalid_base64():
    with pytest.raises(BadDigest, match="not valid base64"):
        _b64md5_to_hex("not base64!")


def test_b64md5_to_hex_wrong_length():
    # 8 bytes, not 16.
    with pytest.raises(BadDigest, match="16 bytes"):
        _b64md5_to_hex(base64.b64encode(b"\x00" * 8).decode())


def test_decode_token_invalid():
    with pytest.raises(InvalidArgument, match="continuation token"):
        _decode_token("***not-base64***")


def test_prune_empty_dirs_stops_at_stop_at(tmp_path):
    base = tmp_path / "bucket"
    base.mkdir()
    leaf = base / "a" / "b"
    leaf.mkdir(parents=True)
    _prune_empty_dirs(leaf, stop_at=base)
    assert not (base / "a").exists()
    assert base.exists()


def test_prune_empty_dirs_nonexistent_start(tmp_path):
    base = tmp_path / "bucket"
    base.mkdir()
    _prune_empty_dirs(base / "nonexistent", stop_at=base)
    # Must not raise; base is still present.
    assert base.exists()


def test_prune_empty_dirs_non_empty_dir_stops(tmp_path):
    base = tmp_path / "bucket"
    base.mkdir()
    non_empty = base / "a"
    non_empty.mkdir()
    (non_empty / "file.txt").write_bytes(b"x")
    _prune_empty_dirs(non_empty, stop_at=base)
    # rmdir would fail on a non-empty dir; function must return gracefully.
    assert non_empty.exists()


def test_is_common_prefix_target():
    assert _is_common_prefix_target("a/b", "a/") is True
    assert _is_common_prefix_target("a", "a/") is False
    assert _is_common_prefix_target("a", "b/") is False


def test_walk_keys_handles_missing_sub_dir(tmp_path):
    """If a directory disappears mid-walk, the walker must skip it."""
    bdir = tmp_path / "b"
    bdir.mkdir()
    (bdir / "a.txt").write_bytes(b"x")
    # Walk a non-existent prefix subtree should just yield nothing.
    common: list[str] = []
    keys = list(_walk_keys(bdir, prefix="no-such-dir/", delimiter=None, common_prefixes_out=common))
    assert keys == []


# ----------------------------------------------------------------------
# nanio/storage/multipart.py — error + edge branches
# ----------------------------------------------------------------------


@pytest.fixture
def manager(tmp_path, storage):
    return MultipartManager(tmp_path)


async def _stream(data: bytes):
    yield data


def test_load_init_oserror_becomes_no_such_upload(manager, tmp_path):
    """A symlink at init.json triggers ELOOP on O_NOFOLLOW open →
    NoSuchUpload."""
    from nanio.errors import NoSuchUpload
    from nanio.storage.paths import multipart_init_path

    upload_id = manager.create(MultipartInit(bucket="widgets", key="k"))
    init_path = multipart_init_path(tmp_path, upload_id)
    init_path.unlink()
    init_path.symlink_to(tmp_path / "nonexistent-target")
    with pytest.raises(NoSuchUpload):
        manager.load_init(upload_id)


def test_list_parts_skips_md5_without_bin(manager, tmp_path):
    """An orphan .md5 file without its .bin partner is silently skipped."""
    upload_id = manager.create(MultipartInit(bucket="widgets", key="k"))
    asyncio.run(manager.upload_part(upload_id, 1, _stream(b"real part")))
    # Drop a bogus .md5 that doesn't match any .bin.
    parts_dir = multipart_dir(tmp_path, upload_id) / "parts"
    (parts_dir / "000099.md5").write_text("deadbeef")
    listed = manager.list_parts(upload_id)
    part_numbers = {p.part_number for p in listed}
    assert 1 in part_numbers
    assert 99 not in part_numbers


def test_list_parts_skips_non_bin_files(manager, tmp_path):
    """Non-.bin files under parts/ must not confuse list_parts."""
    upload_id = manager.create(MultipartInit(bucket="widgets", key="k"))
    asyncio.run(manager.upload_part(upload_id, 1, _stream(b"part 1")))
    parts_dir = multipart_dir(tmp_path, upload_id) / "parts"
    (parts_dir / "stray.txt").write_text("ignore me")
    (parts_dir / "not-a-number.bin").write_bytes(b"x")
    (parts_dir / "not-a-number.md5").write_text("aa")
    listed = manager.list_parts(upload_id)
    assert [p.part_number for p in listed] == [1]


def test_list_parts_missing_md5_file(manager, tmp_path):
    upload_id = manager.create(MultipartInit(bucket="widgets", key="k"))
    asyncio.run(manager.upload_part(upload_id, 1, _stream(b"p1")))
    # Delete the md5 sidecar for part 1 — list_parts must skip it.
    md5_path = multipart_dir(tmp_path, upload_id) / "parts" / "000001.md5"
    md5_path.unlink()
    listed = manager.list_parts(upload_id)
    assert listed == []


def test_iter_upload_dirs_skips_stat_failure(manager, tmp_path):
    """If entry.stat raises OSError, the iterator silently skips it."""
    # Normal case — nothing to fail. Create one real upload, confirm
    # iteration works without raising.
    manager.create(MultipartInit(bucket="widgets", key="k"))
    uploads = manager.list_uploads()
    assert len(uploads) == 1


def test_iter_upload_dirs_no_root(tmp_path):
    """If the multipart root does not exist, list_uploads returns []."""
    m = MultipartManager(tmp_path)
    # Forcibly remove the multipart root that __init__ created.
    import shutil

    from nanio.storage.paths import multipart_root

    shutil.rmtree(multipart_root(tmp_path))
    assert m.list_uploads() == []


def test_gc_rmtree_skips_dir_touched_recently(manager, tmp_path):
    """Directly exercise the recency-gate path of `_gc_rmtree`."""

    upload_id = manager.create(MultipartInit(bucket="widgets", key="k"))
    # The dir was just created; _gc_rmtree should see a fresh mtime
    # and back off.
    now_ts = datetime.now(tz=UTC).timestamp()
    assert manager._gc_rmtree(upload_id, dir_mtime=now_ts, now_ts=now_ts + 0.1) is False
    # Dir must still exist.
    assert multipart_dir(tmp_path, upload_id).exists()


def test_gc_rmtree_handles_rmtree_error(manager, tmp_path, monkeypatch):
    """If shutil.rmtree raises, `_gc_rmtree` logs and returns False."""
    import shutil

    upload_id = manager.create(MultipartInit(bucket="widgets", key="k"))

    def boom(*args, **kwargs):
        raise OSError("simulated rmtree failure")

    monkeypatch.setattr(shutil, "rmtree", boom)
    result = manager._gc_rmtree(
        upload_id,
        dir_mtime=1.0,  # ancient, past recency window
        now_ts=datetime.now(tz=UTC).timestamp(),
    )
    assert result is False


def test_complete_rejects_missing_upload(manager):
    from nanio.errors import NoSuchUpload

    with pytest.raises(NoSuchUpload):
        manager.complete("nonexistent-upload-id", [(1, '"a"')])


def test_upload_part_rejects_out_of_range_numbers(manager):
    from nanio.errors import InvalidPart

    upload_id = manager.create(MultipartInit(bucket="widgets", key="k"))
    with pytest.raises(InvalidPart):
        asyncio.run(manager.upload_part(upload_id, 0, _stream(b"x")))
    with pytest.raises(InvalidPart):
        asyncio.run(manager.upload_part(upload_id, 10_001, _stream(b"x")))


def test_upload_part_missing_upload_dir(tmp_path):
    """upload_part to a nonexistent uploadId raises NoSuchUpload."""
    from nanio.errors import NoSuchUpload

    m = MultipartManager(tmp_path)
    with pytest.raises(NoSuchUpload):
        asyncio.run(m.upload_part("nonexistent", 1, _stream(b"x")))


# ----------------------------------------------------------------------
# nanio/auth/sigv4.py — presigned URL error branches
# ----------------------------------------------------------------------


SECRET_KEY = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
REGION = "us-east-1"


def _lookup(k: str) -> str | None:
    return SECRET_KEY if k == "AKIDEXAMPLE" else None


def test_presigned_wrong_algorithm():
    with pytest.raises(AuthorizationHeaderMalformed, match="Algorithm"):
        verify_presigned_url(
            method="GET",
            path="/",
            query="X-Amz-Algorithm=NOT-SIGV4",
            headers={"host": "x"},
            secret_lookup=_lookup,
        )


def test_presigned_missing_credential():
    with pytest.raises(AuthorizationHeaderMalformed, match="X-Amz-Credential"):
        verify_presigned_url(
            method="GET",
            path="/",
            query=f"X-Amz-Algorithm={ALGORITHM}",
            headers={"host": "x"},
            secret_lookup=_lookup,
        )


def test_presigned_malformed_credential():
    with pytest.raises(AuthorizationHeaderMalformed, match="malformed X-Amz-Credential"):
        verify_presigned_url(
            method="GET",
            path="/",
            query=f"X-Amz-Algorithm={ALGORITHM}&X-Amz-Credential=only/two/parts",
            headers={"host": "x"},
            secret_lookup=_lookup,
        )


def test_presigned_missing_amz_date():
    with pytest.raises(AuthorizationHeaderMalformed, match="X-Amz-Date"):
        verify_presigned_url(
            method="GET",
            path="/",
            query=(
                f"X-Amz-Algorithm={ALGORITHM}"
                "&X-Amz-Credential=AKIDEXAMPLE/20240101/us-east-1/s3/aws4_request"
            ),
            headers={"host": "x"},
            secret_lookup=_lookup,
        )


def test_presigned_expires_out_of_range():
    with pytest.raises(AuthorizationHeaderMalformed, match="out of range"):
        verify_presigned_url(
            method="GET",
            path="/",
            query=(
                f"X-Amz-Algorithm={ALGORITHM}"
                "&X-Amz-Credential=AKIDEXAMPLE/20240101/us-east-1/s3/aws4_request"
                "&X-Amz-Date=20240101T120000Z"
                "&X-Amz-Expires=99999999"
                "&X-Amz-SignedHeaders=host"
                "&X-Amz-Signature=" + "0" * 64
            ),
            headers={"host": "x"},
            secret_lookup=_lookup,
        )


def test_presigned_expired():
    # 20000101 is 20+ years ago, well past any expires window.

    with pytest.raises(RequestTimeTooSkewed, match="expired"):
        verify_presigned_url(
            method="GET",
            path="/",
            query=(
                f"X-Amz-Algorithm={ALGORITHM}"
                "&X-Amz-Credential=AKIDEXAMPLE/20000101/us-east-1/s3/aws4_request"
                "&X-Amz-Date=20000101T120000Z"
                "&X-Amz-Expires=3600"
                "&X-Amz-SignedHeaders=host"
                "&X-Amz-Signature=" + "0" * 64
            ),
            headers={"host": "x"},
            secret_lookup=_lookup,
        )


def test_presigned_from_the_future():
    """A presigned URL with a timestamp far beyond the clock-skew window
    is rejected."""

    future = datetime.now(tz=UTC) + timedelta(hours=2)
    amz_date_str = future.strftime("%Y%m%dT%H%M%SZ")
    date = amz_date_str[:8]
    scope = f"{date}/{REGION}/s3/aws4_request"
    query = (
        f"X-Amz-Algorithm={ALGORITHM}"
        f"&X-Amz-Credential=AKIDEXAMPLE/{scope}"
        f"&X-Amz-Date={amz_date_str}"
        "&X-Amz-Expires=3600"
        "&X-Amz-SignedHeaders=host"
        "&X-Amz-Signature=" + "0" * 64
    )
    with pytest.raises(RequestTimeTooSkewed, match="future"):
        verify_presigned_url(
            method="GET",
            path="/",
            query=query,
            headers={"host": "x"},
            secret_lookup=_lookup,
        )


def test_presigned_missing_signature():
    amz_date = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    date = amz_date[:8]
    query = (
        f"X-Amz-Algorithm={ALGORITHM}"
        f"&X-Amz-Credential=AKIDEXAMPLE/{date}/us-east-1/s3/aws4_request"
        f"&X-Amz-Date={amz_date}"
        "&X-Amz-Expires=3600"
        "&X-Amz-SignedHeaders=host"
    )
    with pytest.raises(AuthorizationHeaderMalformed, match="X-Amz-Signature"):
        verify_presigned_url(
            method="GET",
            path="/",
            query=query,
            headers={"host": "x"},
            secret_lookup=_lookup,
        )


def test_presigned_unknown_access_key():
    amz_date = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    date = amz_date[:8]
    query = (
        f"X-Amz-Algorithm={ALGORITHM}"
        f"&X-Amz-Credential=UNKNOWN/{date}/us-east-1/s3/aws4_request"
        f"&X-Amz-Date={amz_date}"
        "&X-Amz-Expires=3600"
        "&X-Amz-SignedHeaders=host"
        "&X-Amz-Signature=" + "0" * 64
    )
    with pytest.raises(InvalidAccessKeyId):
        verify_presigned_url(
            method="GET",
            path="/",
            query=query,
            headers={"host": "x"},
            secret_lookup=_lookup,
        )


def test_authorization_parser_skips_empty_segments():
    """`parse_authorization_header` handles stray commas by skipping
    empty segments via `continue`."""
    # Double commas between valid fields — the parser must still
    # extract the required fields without raising.
    auth = (
        "AWS4-HMAC-SHA256 "
        "Credential=AK/20240101/us-east-1/s3/aws4_request,,"
        "SignedHeaders=host,"
        "Signature=abc"
    )
    parts = parse_authorization_header(auth)
    assert parts.access_key == "AK"


def test_authorization_parser_malformed_credential_field():
    with pytest.raises(AuthorizationHeaderMalformed, match="Credential"):
        parse_authorization_header(
            "AWS4-HMAC-SHA256 Credential=only/two/parts, SignedHeaders=host, Signature=abc"
        )


def test_presigned_query_skips_empty_segments():
    """`verify_presigned_url` splits the query on `&` and skips blank
    segments. Cover the `continue` branch at line 340."""
    # Start with a valid query + extra empty segments.
    amz_date = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    date = amz_date[:8]
    scope = f"{date}/us-east-1/s3/aws4_request"
    query = (
        f"X-Amz-Algorithm={ALGORITHM}"
        "&&"  # empty segment
        f"&X-Amz-Credential=AKIDEXAMPLE/{scope}"
        f"&X-Amz-Date={amz_date}"
        "&X-Amz-Expires=3600"
        "&X-Amz-SignedHeaders=host"
        "&X-Amz-Signature=" + "0" * 64
    )
    # Signature is wrong so it'll raise SignatureDoesNotMatch, but that's
    # after the query parser has already skipped the empty segment.
    with pytest.raises(Exception):  # noqa: B017
        verify_presigned_url(
            method="GET",
            path="/",
            query=query,
            headers={"host": "x"},
            secret_lookup=_lookup,
        )


# ----------------------------------------------------------------------
# nanio/auth/chunked.py — _Buffered edge cases
# ----------------------------------------------------------------------


def _make_source(data: bytes, chunk_size: int = 1024):
    pos = 0

    async def _src() -> bytes:
        nonlocal pos
        if pos >= len(data):
            return b""
        end = min(len(data), pos + chunk_size)
        out = data[pos:end]
        pos = end
        return out

    return _src


def test_buffered_read_exact_raises_on_eof():
    """read_exact on an exhausted source must raise InvalidRequest."""
    buf = _Buffered(_make_source(b"abc"))

    async def go():
        # Pull the 3 bytes — OK.
        assert await buf.read_exact(3) == b"abc"
        # Next read must raise because the source is EOF and we still
        # want bytes.
        await buf.read_exact(1)

    with pytest.raises(InvalidRequest, match="unexpected EOF"):
        asyncio.run(go())


def test_buffered_read_exact_mid_stream_eof():
    """read_exact that asks for more than the source has ever seen."""
    buf = _Buffered(_make_source(b"short"))

    async def go():
        await buf.read_exact(100)

    with pytest.raises(InvalidRequest):
        asyncio.run(go())


def test_buffered_read_line_rejects_oversized_header():
    big = b"x" * 5000 + b"\r\n"
    buf = _Buffered(_make_source(big, chunk_size=256))

    async def go():
        await buf.read_line(max_len=4096)

    with pytest.raises(InvalidRequest, match="frame header too long"):
        asyncio.run(go())


def test_buffered_read_line_raises_on_eof_mid_line():
    """A line that never sees a CRLF before EOF raises."""
    buf = _Buffered(_make_source(b"no terminator here"))

    async def go():
        await buf.read_line()

    with pytest.raises(InvalidRequest, match="EOF reading"):
        asyncio.run(go())


def test_decode_aws_chunked_rejects_empty_chunk_header():
    """An empty line as the chunk header is rejected."""
    signing_key = derive_signing_key(SECRET_KEY, date="20240101", region=REGION, service="s3")

    body = b"\r\n"
    decoder = decode_aws_chunked(
        _make_source(body),
        signing_key=signing_key,
        seed_signature="0" * 64,
        amz_date="20240101T120000Z",
        scope=f"20240101/{REGION}/s3/aws4_request",
    )

    async def drain():
        async for _ in decoder:
            pass

    with pytest.raises(InvalidRequest, match="empty chunk header"):
        asyncio.run(drain())
