"""Multipart upload state on the filesystem.

Each in-progress multipart upload occupies a directory under
`<data-dir>/.nanio/multipart/<upload_id>/` with this layout:

    init.json              — target bucket, key, content-type, user metadata
    parts/000001.bin       — uploaded part data
    parts/000001.md5       — hex md5 of the part bytes
    parts/000002.bin
    parts/000002.md5
    ...
    assembled.tmp          — scratch file used during Complete (deleted on success)

`uploadId` values are random base64 strings (`secrets.token_urlsafe(24)`),
so they encode no server state — any worker on the shared filesystem can
service any subsequent request for the same upload.
"""

from __future__ import annotations

import asyncio
import json
import os
import secrets
import shutil
from collections.abc import AsyncIterator, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from nanio.errors import (
    InvalidPart,
    InvalidPartOrder,
    NoSuchUpload,
)
from nanio.etag import StreamingMd5, multipart_etag, quote_etag, unquote_etag
from nanio.storage.backend import ObjectInfo
from nanio.storage.metadata import write_metadata
from nanio.storage.paths import (
    metadata_path,
    multipart_dir,
    multipart_init_path,
    multipart_part_md5_path,
    multipart_part_path,
    multipart_root,
    object_path,
)


@dataclass(slots=True)
class MultipartInit:
    bucket: str
    key: str
    content_type: str = "application/octet-stream"
    user_metadata: Mapping[str, str] = field(default_factory=dict)
    content_encoding: str | None = None
    content_disposition: str | None = None
    cache_control: str | None = None
    initiated: datetime = field(default_factory=lambda: datetime.now(tz=UTC))


@dataclass(slots=True)
class PartInfo:
    part_number: int
    etag: str  # quoted hex md5
    size: int
    last_modified: datetime


def new_upload_id() -> str:
    return secrets.token_urlsafe(24)


class MultipartManager:
    """Filesystem-backed multipart upload state.

    All methods take an explicit `data_dir` so the manager itself remains
    stateless and any worker process can be passed to it. Tests construct
    one per `tmp_path`.
    """

    def __init__(self, data_dir: Path, *, chunk_size: int = 1024 * 1024) -> None:
        self._data_dir = data_dir
        self._chunk_size = chunk_size
        multipart_root(data_dir).mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Create / abort
    # ------------------------------------------------------------------

    def create(self, init: MultipartInit) -> str:
        upload_id = new_upload_id()
        d = multipart_dir(self._data_dir, upload_id)
        (d / "parts").mkdir(parents=True, exist_ok=False)
        with open(multipart_init_path(self._data_dir, upload_id), "w", encoding="utf-8") as f:
            json.dump(_init_to_dict(init), f)
        return upload_id

    def abort(self, upload_id: str) -> None:
        d = multipart_dir(self._data_dir, upload_id)
        if not d.is_dir():
            raise NoSuchUpload(resource=upload_id)
        shutil.rmtree(d)

    def load_init(self, upload_id: str) -> MultipartInit:
        path = multipart_init_path(self._data_dir, upload_id)
        if not path.is_file():
            raise NoSuchUpload(resource=upload_id)
        with open(path, encoding="utf-8") as f:
            return _dict_to_init(json.load(f))

    # ------------------------------------------------------------------
    # Upload part (streaming)
    # ------------------------------------------------------------------

    async def upload_part(
        self,
        upload_id: str,
        part_number: int,
        body: AsyncIterator[bytes],
    ) -> PartInfo:
        if part_number < 1 or part_number > 10_000:
            raise InvalidPart(f"part number out of range: {part_number}")
        if not multipart_dir(self._data_dir, upload_id).is_dir():
            raise NoSuchUpload(resource=upload_id)

        target = multipart_part_path(self._data_dir, upload_id, part_number)
        md5_target = multipart_part_md5_path(self._data_dir, upload_id, part_number)
        target.parent.mkdir(parents=True, exist_ok=True)

        tmp = target.with_suffix(".bin.tmp")
        hasher = StreamingMd5()
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
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

        os.replace(tmp, target)
        md5_target.write_text(hasher.hexdigest())

        return PartInfo(
            part_number=part_number,
            etag=hasher.quoted_etag(),
            size=hasher.size,
            last_modified=datetime.now(tz=UTC),
        )

    # ------------------------------------------------------------------
    # List parts / list uploads
    # ------------------------------------------------------------------

    def list_parts(self, upload_id: str) -> list[PartInfo]:
        d = multipart_dir(self._data_dir, upload_id)
        if not d.is_dir():
            raise NoSuchUpload(resource=upload_id)
        parts_dir = d / "parts"
        if not parts_dir.is_dir():
            return []
        out: list[PartInfo] = []
        for entry in sorted(os.scandir(parts_dir), key=lambda e: e.name):
            if not entry.name.endswith(".bin"):
                continue
            stem = entry.name[:-4]  # strip .bin
            try:
                pn = int(stem)
            except ValueError:
                continue
            md5_path = parts_dir / f"{stem}.md5"
            if not md5_path.is_file():
                continue
            md5_hex = md5_path.read_text().strip()
            stat = entry.stat()
            out.append(
                PartInfo(
                    part_number=pn,
                    etag=quote_etag(md5_hex),
                    size=stat.st_size,
                    last_modified=datetime.fromtimestamp(stat.st_mtime, tz=UTC),
                )
            )
        return out

    def list_uploads(self) -> list[tuple[str, MultipartInit]]:
        root = multipart_root(self._data_dir)
        if not root.is_dir():
            return []
        out: list[tuple[str, MultipartInit]] = []
        for entry in os.scandir(root):
            if not entry.is_dir():
                continue
            try:
                init = self.load_init(entry.name)
            except NoSuchUpload:
                continue
            out.append((entry.name, init))
        return out

    def warn_about_abandoned_uploads(
        self, *, max_age_seconds: int = 7 * 24 * 3600
    ) -> list[tuple[str, datetime]]:
        """Return (upload_id, initiated) for uploads older than `max_age_seconds`.

        Caller logs the warnings — we keep this pure so unit tests can
        assert on the result.
        """
        now = datetime.now(tz=UTC)
        old: list[tuple[str, datetime]] = []
        for upload_id, init in self.list_uploads():
            age = (now - init.initiated).total_seconds()
            if age > max_age_seconds:
                old.append((upload_id, init.initiated))
        return old

    # ------------------------------------------------------------------
    # Complete
    # ------------------------------------------------------------------

    def complete(
        self,
        upload_id: str,
        client_parts: Iterable[tuple[int, str]],  # (part_number, etag)
    ) -> ObjectInfo:
        d = multipart_dir(self._data_dir, upload_id)
        if not d.is_dir():
            raise NoSuchUpload(resource=upload_id)
        init = self.load_init(upload_id)

        client_parts_list = list(client_parts)
        if not client_parts_list:
            raise InvalidPart("Complete requested with no parts")

        # Parts must be ascending and unique.
        prev_pn = 0
        for pn, _ in client_parts_list:
            if pn <= prev_pn:
                raise InvalidPartOrder()
            prev_pn = pn

        # Verify each requested part exists with the matching etag.
        on_disk_parts: list[tuple[int, Path, str, int]] = []  # (pn, path, md5_hex, size)
        for pn, requested_etag in client_parts_list:
            path = multipart_part_path(self._data_dir, upload_id, pn)
            md5_path = multipart_part_md5_path(self._data_dir, upload_id, pn)
            if not path.is_file() or not md5_path.is_file():
                raise InvalidPart(f"missing part {pn}")
            md5_hex = md5_path.read_text().strip()
            requested_hex = unquote_etag(requested_etag)
            if requested_hex != md5_hex:
                raise InvalidPart(f"etag mismatch for part {pn}")
            on_disk_parts.append((pn, path, md5_hex, path.stat().st_size))

        # Concatenate parts into an assembled scratch file inside the upload dir.
        assembled = d / "assembled.tmp"
        with open(assembled, "wb") as out_f:
            out_fd = out_f.fileno()
            for _, path, _, size in on_disk_parts:
                with open(path, "rb") as in_f:
                    in_fd = in_f.fileno()
                    remaining = size
                    offset = 0
                    while remaining > 0:
                        sent = os.sendfile(out_fd, in_fd, offset, remaining)
                        if sent == 0:
                            break
                        offset += sent
                        remaining -= sent
            out_f.flush()
            os.fsync(out_fd)

        # Final etag is the multipart format.
        final_etag = multipart_etag(md5 for _, _, md5, _ in on_disk_parts)
        total_size = sum(size for _, _, _, size in on_disk_parts)

        # Move assembled file into the bucket. Atomic on the same FS.
        final_path = object_path(self._data_dir, init.bucket, init.key)
        final_path.parent.mkdir(parents=True, exist_ok=True)
        os.replace(assembled, final_path)

        info = ObjectInfo(
            key=init.key,
            size=total_size,
            etag=final_etag,
            last_modified=datetime.now(tz=UTC),
            content_type=init.content_type,
            user_metadata=dict(init.user_metadata),
            content_encoding=init.content_encoding,
            content_disposition=init.content_disposition,
            cache_control=init.cache_control,
        )
        write_metadata(metadata_path(self._data_dir, init.bucket, init.key), info)

        # Best-effort cleanup of the upload dir.
        shutil.rmtree(d, ignore_errors=True)
        return info


# ----------------------------------------------------------------------
# Init dict (de)serialization
# ----------------------------------------------------------------------


def _init_to_dict(init: MultipartInit) -> dict:
    return {
        "bucket": init.bucket,
        "key": init.key,
        "content_type": init.content_type,
        "user_metadata": dict(init.user_metadata),
        "content_encoding": init.content_encoding,
        "content_disposition": init.content_disposition,
        "cache_control": init.cache_control,
        "initiated": init.initiated.isoformat(),
    }


def _dict_to_init(d: dict) -> MultipartInit:
    return MultipartInit(
        bucket=d["bucket"],
        key=d["key"],
        content_type=d.get("content_type") or "application/octet-stream",
        user_metadata=dict(d.get("user_metadata") or {}),
        content_encoding=d.get("content_encoding"),
        content_disposition=d.get("content_disposition"),
        cache_control=d.get("cache_control"),
        initiated=datetime.fromisoformat(d["initiated"]),
    )
