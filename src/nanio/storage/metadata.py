"""Sidecar JSON metadata read/write.

Each object's metadata lives at `<bucket>/.nanio-meta/<key>.json`. Writes
go through a tmp-then-rename pattern so that a crash mid-write never
leaves a partially-written sidecar.
"""

from __future__ import annotations

import contextlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path

from nanio.storage.backend import ObjectInfo


def _to_dict(info: ObjectInfo) -> dict:
    return {
        "etag": info.etag,
        "size": info.size,
        "last_modified": info.last_modified.isoformat(),
        "content_type": info.content_type,
        "user_metadata": dict(info.user_metadata),
        "storage_class": info.storage_class,
        "content_encoding": info.content_encoding,
        "content_disposition": info.content_disposition,
        "cache_control": info.cache_control,
    }


def _from_dict(key: str, d: dict) -> ObjectInfo:
    return ObjectInfo(
        key=key,
        size=int(d["size"]),
        etag=str(d["etag"]),
        last_modified=datetime.fromisoformat(d["last_modified"]),
        content_type=str(d.get("content_type") or "application/octet-stream"),
        user_metadata=dict(d.get("user_metadata") or {}),
        storage_class=str(d.get("storage_class") or "STANDARD"),
        content_encoding=d.get("content_encoding"),
        content_disposition=d.get("content_disposition"),
        cache_control=d.get("cache_control"),
    )


def write_metadata(path: Path, info: ObjectInfo) -> None:
    """Atomically write the sidecar JSON file at `path`.

    Uses O_NOFOLLOW so a pre-existing symlink at the sidecar path is not
    silently followed (security audit finding H5). Combined with O_EXCL
    on the temp file, the writer always owns the inode it just created.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(_to_dict(info), separators=(",", ":")).encode("utf-8")
    tmp = path.with_suffix(path.suffix + ".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o644)
    try:
        with os.fdopen(fd, "wb", closefd=True) as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(tmp)
        raise
    os.replace(tmp, path)


def read_metadata(path: Path, key: str) -> ObjectInfo:
    """Load and parse the sidecar JSON file at `path`.

    Refuses to follow a leaf symlink so a malicious sidecar replacement
    cannot trick the listing into reading an arbitrary file.
    """
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, "rb", closefd=True) as f:
        d = json.loads(f.read().decode("utf-8"))
    return _from_dict(key, d)


def synthesize_metadata_from_stat(path: Path, key: str) -> ObjectInfo:
    """Fall-back for objects whose sidecar is missing — derive what we can.

    Used as a safety net so that data files placed by hand still appear in
    listings, with sensible defaults for content type and ETag.
    """
    st = path.stat()
    last_mod = datetime.fromtimestamp(st.st_mtime, tz=UTC)
    return ObjectInfo(
        key=key,
        size=st.st_size,
        etag='""',  # unknown — empty quoted string
        last_modified=last_mod,
    )
