"""Filesystem-backed storage implementation.

The single backend nanio ships with in v1. All path math goes through
`storage.paths`. All metadata I/O goes through `storage.metadata`. This
module owns the file-streaming hot path.

Streaming rules:
- PUT: write to a temp file with `os.write` chunk-by-chunk, hashing as we
  go, then atomic `os.replace` to the final path.
- GET: return an async generator that does `os.pread` chunk-by-chunk.
- Never `read()` a whole file into memory.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import logging
import os
import shutil
import uuid
from collections.abc import AsyncIterator, Iterator, Mapping
from datetime import UTC, datetime
from pathlib import Path

from nanio.errors import (
    BadDigest,
    BucketAlreadyOwnedByYou,
    BucketNotEmpty,
    InvalidArgument,
    NoSuchBucket,
    NoSuchKey,
)
from nanio.etag import StreamingMd5
from nanio.keys import validate_bucket_name, validate_object_key
from nanio.storage.backend import (
    BucketInfo,
    GetObjectResult,
    ListResult,
    ObjectInfo,
)
from nanio.storage.metadata import (
    read_metadata,
    synthesize_metadata_from_stat,
    write_metadata,
)
from nanio.storage.paths import (
    assert_inside_data_dir,
    bucket_dir,
    is_internal_name,
    metadata_path,
    metadata_root,
    multipart_root,
    object_path,
)

_log = logging.getLogger("nanio.storage")


class FilesystemStorage:
    """Concrete `Storage` backed by a POSIX filesystem rooted at `data_dir`.

    Holds no in-process state — every method derives everything from the
    filesystem. Safe to share across worker processes via shared storage.
    """

    def __init__(self, data_dir: Path, *, chunk_size: int = 1024 * 1024) -> None:
        self._data_dir = data_dir.resolve()
        self._chunk_size = chunk_size
        self._data_dir.mkdir(parents=True, exist_ok=True)
        # Reserve the multipart root upfront so it always exists.
        multipart_root(self._data_dir).mkdir(parents=True, exist_ok=True)

    @property
    def data_dir(self) -> Path:
        return self._data_dir

    # ------------------------------------------------------------------
    # Bucket operations
    # ------------------------------------------------------------------

    def create_bucket(self, bucket: str) -> BucketInfo:
        validate_bucket_name(bucket)
        bdir = bucket_dir(self._data_dir, bucket)
        try:
            bdir.mkdir(parents=False, exist_ok=False)
        except FileExistsError as exc:
            raise BucketAlreadyOwnedByYou(resource=bucket) from exc
        # Pre-create the metadata sub-tree so listings can rely on it.
        metadata_root(self._data_dir, bucket).mkdir(parents=True, exist_ok=True)
        return self._bucket_info(bucket)

    def delete_bucket(self, bucket: str) -> None:
        bdir = self._require_bucket_dir(bucket)
        # The bucket is "empty" if the only entries are nanio internals.
        for entry in os.scandir(bdir):
            if not is_internal_name(entry.name):
                raise BucketNotEmpty(resource=bucket)
        # Also confirm the metadata tree contains no leftover files
        # (which would mean orphan metadata for already-deleted objects;
        # safe to delete here since the bucket is otherwise empty).
        shutil.rmtree(bdir)

    def head_bucket(self, bucket: str) -> BucketInfo:
        self._require_bucket_dir(bucket)
        return self._bucket_info(bucket)

    def list_buckets(self) -> list[BucketInfo]:
        out: list[BucketInfo] = []
        for entry in os.scandir(self._data_dir):
            if not entry.is_dir(follow_symlinks=False) or is_internal_name(entry.name):
                continue
            try:
                validate_bucket_name(entry.name)
            except Exception:
                continue
            out.append(self._bucket_info(entry.name))
        out.sort(key=lambda b: b.name)
        return out

    def _bucket_info(self, bucket: str) -> BucketInfo:
        st = bucket_dir(self._data_dir, bucket).stat()
        return BucketInfo(
            name=bucket,
            created=datetime.fromtimestamp(st.st_ctime, tz=UTC),
        )

    def _require_bucket_dir(self, bucket: str) -> Path:
        validate_bucket_name(bucket)
        bdir = bucket_dir(self._data_dir, bucket)
        if not self._is_safe_bucket_dir(bdir):
            raise NoSuchBucket(resource=bucket)
        return bdir

    def _is_safe_bucket_dir(self, bdir: Path) -> bool:
        if not bdir.is_dir() or bdir.is_symlink():
            return False
        return not _path_escapes(self._data_dir, bdir)

    # ------------------------------------------------------------------
    # Object operations
    # ------------------------------------------------------------------

    async def put_object(
        self,
        bucket: str,
        key: str,
        body: AsyncIterator[bytes],
        *,
        content_type: str = "application/octet-stream",
        user_metadata: Mapping[str, str] | None = None,
        content_encoding: str | None = None,
        content_disposition: str | None = None,
        cache_control: str | None = None,
        expected_md5: str | None = None,
    ) -> ObjectInfo:
        self._require_bucket_dir(bucket)
        validate_object_key(key)

        final = object_path(self._data_dir, bucket, key)
        # Refuse to create the parent directory tree if any existing
        # component is a symlink leading outside the data dir (security
        # audit finding H5).
        if final.parent.exists():
            assert_inside_data_dir(self._data_dir, final.parent)
        final.parent.mkdir(parents=True, exist_ok=True)
        # Re-check after mkdir in case mkdir followed a symlink.
        assert_inside_data_dir(self._data_dir, final.parent)

        # Stage to a temp file inside the bucket so the rename is on the
        # same filesystem and therefore atomic.
        tmp = final.parent / f".tmp.{uuid.uuid4().hex}"

        hasher = StreamingMd5()
        # O_NOFOLLOW means that if `tmp` somehow already exists as a
        # symlink (race window), open fails with ELOOP. Combined with
        # O_EXCL it's a strong guarantee we own the inode we just created.
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o644)
        try:
            try:
                async for chunk in body:
                    if not chunk:
                        continue
                    await asyncio.to_thread(os.write, fd, chunk)
                    hasher.update(chunk)
            finally:
                os.fsync(fd)
                os.close(fd)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise

        if expected_md5 is not None:
            # Content-MD5 header is base64 of the binary digest.
            expected_hex = _b64md5_to_hex(expected_md5)
            if expected_hex != hasher.hexdigest():
                tmp.unlink(missing_ok=True)
                raise BadDigest()

        os.replace(tmp, final)

        info = ObjectInfo(
            key=key,
            size=hasher.size,
            etag=hasher.quoted_etag(),
            last_modified=datetime.now(tz=UTC),
            content_type=content_type,
            user_metadata=dict(user_metadata or {}),
            content_encoding=content_encoding,
            content_disposition=content_disposition,
            cache_control=cache_control,
        )
        write_metadata(metadata_path(self._data_dir, bucket, key), info)
        return info

    def head_object(self, bucket: str, key: str) -> ObjectInfo:
        self._require_bucket_dir(bucket)
        validate_object_key(key)
        opath = object_path(self._data_dir, bucket, key)
        # Refuse to read through any symlink that escapes the data dir.
        if opath.is_symlink() or (opath.exists() and _path_escapes(self._data_dir, opath)):
            raise NoSuchKey(resource=f"{bucket}/{key}")
        if not opath.is_file():
            raise NoSuchKey(resource=f"{bucket}/{key}")
        meta = self._read_or_synthesize_metadata(bucket, key, opath)
        return meta

    async def get_object(
        self,
        bucket: str,
        key: str,
        *,
        range_start: int | None = None,
        range_end: int | None = None,
    ) -> GetObjectResult:
        info = self.head_object(bucket, key)
        opath = object_path(self._data_dir, bucket, key)
        size = info.size

        if range_start is None and range_end is None:
            start = 0
            end = size - 1
            length = size
        else:
            start = range_start if range_start is not None else 0
            end = range_end if range_end is not None else size - 1
            if start < 0 or end >= size or start > end:
                raise InvalidArgument(f"invalid byte range {start}-{end} for size {size}")
            length = end - start + 1

        body = _stream_pread(opath, start, length, self._chunk_size)
        return GetObjectResult(
            info=info,
            body=body,
            range_start=start if (range_start is not None or range_end is not None) else None,
            range_end=end if (range_start is not None or range_end is not None) else None,
        )

    def delete_object(self, bucket: str, key: str) -> None:
        self._require_bucket_dir(bucket)
        validate_object_key(key)
        opath = object_path(self._data_dir, bucket, key)
        # S3 DELETE is idempotent — deleting a non-existent key is success.
        with contextlib.suppress(FileNotFoundError):
            os.remove(opath)
        with contextlib.suppress(FileNotFoundError):
            os.remove(metadata_path(self._data_dir, bucket, key))
        # Best-effort: prune empty parent dirs (but never the bucket dir itself).
        _prune_empty_dirs(opath.parent, stop_at=bucket_dir(self._data_dir, bucket))

    def list_objects(
        self,
        bucket: str,
        *,
        prefix: str = "",
        delimiter: str | None = None,
        max_keys: int = 1000,
        continuation_token: str | None = None,
        start_after: str | None = None,
    ) -> ListResult:
        bdir = self._require_bucket_dir(bucket)
        if max_keys < 0:
            raise InvalidArgument("max-keys must be >= 0")
        max_keys = min(max_keys, 1000)

        # Decode the resume marker, if any.
        resume_after = _decode_token(continuation_token) if continuation_token else None
        if resume_after is None and start_after:
            resume_after = start_after

        contents: list[ObjectInfo] = []
        common: list[str] = []
        is_truncated = False
        next_token: str | None = None

        if max_keys == 0:
            return ListResult([], [], False, None)

        for full_key in _walk_keys(bdir, prefix, delimiter, common):
            if resume_after is not None and full_key <= resume_after:
                continue
            opath = object_path(self._data_dir, bucket, full_key)
            try:
                info = self._read_or_synthesize_metadata(bucket, full_key, opath)
            except (NoSuchKey, FileNotFoundError):
                # Object disappeared between scandir and metadata read; skip it.
                continue
            except (ValueError, KeyError, OSError) as exc:
                # Sidecar JSON is corrupt / missing fields / unreadable. Don't
                # let one bad sidecar take down the whole listing — fall back
                # to synthesized metadata so the object still appears.
                _log.warning(
                    "bucket=%s key=%s: sidecar metadata unreadable (%s); using synthesized",
                    bucket,
                    full_key,
                    exc,
                )
                if not opath.is_file():
                    continue
                info = synthesize_metadata_from_stat(opath, full_key)
            contents.append(info)
            if len(contents) >= max_keys:
                is_truncated = True
                next_token = _encode_token(full_key)
                break

        contents.sort(key=lambda c: c.key)
        common.sort()
        # Common prefixes that fall under `start_after` should still be
        # included by S3 semantics, so we don't filter them.

        return ListResult(
            contents=contents,
            common_prefixes=common,
            is_truncated=is_truncated,
            next_continuation_token=next_token,
        )

    async def copy_object(
        self,
        src_bucket: str,
        src_key: str,
        dst_bucket: str,
        dst_key: str,
    ) -> ObjectInfo:
        src_info = self.head_object(src_bucket, src_key)
        src_path = object_path(self._data_dir, src_bucket, src_key)

        async def _stream() -> AsyncIterator[bytes]:
            # Reuse the streaming reader so we never load the source body.
            for chunk in _sync_iter_file(src_path, self._chunk_size):
                yield chunk

        return await self.put_object(
            dst_bucket,
            dst_key,
            _stream(),
            content_type=src_info.content_type,
            user_metadata=dict(src_info.user_metadata),
            content_encoding=src_info.content_encoding,
            content_disposition=src_info.content_disposition,
            cache_control=src_info.cache_control,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _read_or_synthesize_metadata(self, bucket: str, key: str, opath: Path) -> ObjectInfo:
        mpath = metadata_path(self._data_dir, bucket, key)
        try:
            return read_metadata(mpath, key)
        except FileNotFoundError:
            if not opath.is_file():
                raise NoSuchKey(resource=f"{bucket}/{key}") from None
            return synthesize_metadata_from_stat(opath, key)
        except (ValueError, KeyError, OSError) as exc:
            # Sidecar exists but can't be read or parsed (symlink with
            # O_NOFOLLOW → ELOOP, malformed JSON, missing fields). Don't
            # trust the file — fall back to synthesized metadata derived
            # from the real object's stat.
            _log.warning(
                "bucket=%s key=%s: sidecar metadata unreadable (%s); using synthesized",
                bucket,
                key,
                exc,
            )
            if not opath.is_file():
                raise NoSuchKey(resource=f"{bucket}/{key}") from None
            return synthesize_metadata_from_stat(opath, key)


# ----------------------------------------------------------------------
# Module-level helpers (kept module-private)
# ----------------------------------------------------------------------


def _b64md5_to_hex(b64: str) -> str:
    try:
        raw = base64.b64decode(b64, validate=True)
    except Exception as exc:
        raise BadDigest("Content-MD5 header is not valid base64") from exc
    if len(raw) != 16:
        raise BadDigest("Content-MD5 must be 16 bytes")
    return raw.hex()


def _encode_token(key: str) -> str:
    return base64.urlsafe_b64encode(key.encode("utf-8")).decode("ascii")


def _decode_token(token: str) -> str:
    try:
        return base64.urlsafe_b64decode(token.encode("ascii")).decode("utf-8")
    except Exception as exc:
        raise InvalidArgument(f"invalid continuation token: {token!r}") from exc


def _stream_pread(path: Path, offset: int, length: int, chunk_size: int) -> AsyncIterator[bytes]:
    """Async generator that streams `length` bytes starting at `offset`."""

    async def _gen() -> AsyncIterator[bytes]:
        # O_NOFOLLOW so a leaf symlink can never be opened — defends
        # against symlink-target disclosure (security audit H5).
        fd = await asyncio.to_thread(os.open, path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            remaining = length
            pos = offset
            while remaining > 0:
                read_size = min(chunk_size, remaining)
                chunk = await asyncio.to_thread(os.pread, fd, read_size, pos)
                if not chunk:
                    break
                yield chunk
                pos += len(chunk)
                remaining -= len(chunk)
        finally:
            await asyncio.to_thread(os.close, fd)

    return _gen()


def _sync_iter_file(path: Path, chunk_size: int) -> Iterator[bytes]:
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        with os.fdopen(fd, "rb", closefd=True) as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    return
                yield chunk
    except BaseException:
        with contextlib.suppress(OSError):
            os.close(fd)
        raise


def _path_escapes(data_dir: Path, target: Path) -> bool:
    """Return True if `target` realpaths to a location outside `data_dir`."""
    try:
        assert_inside_data_dir(data_dir, target)
    except PermissionError:
        return True
    return False


def _prune_empty_dirs(start: Path, *, stop_at: Path) -> None:
    """Walk up from `start`, removing empty dirs until we hit `stop_at`."""
    cur = start
    stop_at = stop_at.resolve()
    while True:
        try:
            cur_resolved = cur.resolve()
        except FileNotFoundError:  # pragma: no cover
            # Defensive: the current dir may disappear concurrently.
            return
        if cur_resolved == stop_at or stop_at not in cur_resolved.parents:
            return
        try:
            cur.rmdir()
        except OSError:
            return
        cur = cur.parent


def _walk_keys(
    bdir: Path,
    prefix: str,
    delimiter: str | None,
    common_prefixes_out: list[str],
) -> Iterator[str]:
    """Yield object keys under `bdir`, applying prefix and delimiter rules.

    For v1 we always do a depth-first scandir of the bucket, filter by
    prefix, and (when delimiter == "/") collapse subtrees into
    `CommonPrefixes`. This is sufficient for the listing semantics that
    boto3 / aws-cli care about and stays simple.

    Returns an iterator of object keys (sorted within each directory).
    Common prefixes are appended to `common_prefixes_out` in arbitrary
    order; the caller sorts them.
    """
    seen_prefixes: set[str] = set()

    def _walk(rel_dir: str) -> Iterator[str]:
        sub = bdir if not rel_dir else bdir / rel_dir
        try:
            entries = sorted(os.scandir(sub), key=lambda e: e.name)
        except FileNotFoundError:  # pragma: no cover
            # Defensive against a concurrent rmdir between the parent
            # scandir (which yielded this dir) and our descent into it.
            return
        for entry in entries:
            if is_internal_name(entry.name):
                continue
            rel_path = f"{rel_dir}/{entry.name}" if rel_dir else entry.name
            if entry.is_dir(follow_symlinks=False):
                # If the rel_path doesn't even share the prefix's first
                # path segment, skip the whole subtree.
                if prefix and not (
                    rel_path.startswith(prefix) or prefix.startswith(rel_path + "/")
                ):
                    continue
                if delimiter == "/" and prefix:
                    # If we're inside a directory whose path is past the
                    # last `/` of the prefix, treat it as a CommonPrefix.
                    if _is_common_prefix_target(rel_path, prefix):
                        cp = rel_path + "/"
                        if cp not in seen_prefixes:  # pragma: no branch
                            seen_prefixes.add(cp)
                            common_prefixes_out.append(cp)
                        continue
                elif delimiter == "/" and not prefix:
                    cp = rel_path + "/"
                    if cp not in seen_prefixes:  # pragma: no branch
                        seen_prefixes.add(cp)
                        common_prefixes_out.append(cp)
                    continue
                yield from _walk(rel_path)
            else:
                if not rel_path.startswith(prefix):
                    continue
                yield rel_path

    yield from _walk("")


def _is_common_prefix_target(rel_path: str, prefix: str) -> bool:
    """Whether a directory at `rel_path` should collapse into a CommonPrefix.

    Used only when `delimiter == "/"`. The directory collapses if there is
    at least one path segment AFTER the prefix.
    """
    if not rel_path.startswith(prefix):
        # The directory is part of the prefix's parent chain — keep walking.
        return False
    remainder = rel_path[len(prefix) :]
    return bool(remainder)
