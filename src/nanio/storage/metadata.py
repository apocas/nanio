"""Sidecar JSON metadata read/write.

Each object's metadata lives at `<bucket>/.nanio-meta/<key>.json`. Writes
go through a tmp-then-rename pattern so that a crash mid-write never
leaves a partially-written sidecar.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path

from nanio.storage.backend import ObjectInfo
from nanio.storage.paths import atomic_write


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

    Goes through the shared `atomic_write` helper so the write can never
    leave a partial file on disk after a crash, and so O_NOFOLLOW refuses
    to follow any pre-existing symlink at the sidecar path.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(_to_dict(info), separators=(",", ":")).encode("utf-8")
    with atomic_write(path) as fd:
        os.write(fd, payload)


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

    Uses ``lstat`` (``follow_symlinks=False``) so that if ``path`` is a
    symlink, we report the symlink's own metadata rather than the target's.
    Without this, a corrupt or symlinked sidecar could force the listing
    path into this fallback and leak the size and mtime of a file outside
    the bucket.
    """
    st = path.stat(follow_symlinks=False)
    last_mod = datetime.fromtimestamp(st.st_mtime, tz=UTC)
    return ObjectInfo(
        key=key,
        size=st.st_size,
        etag='""',  # unknown — empty quoted string
        last_modified=last_mod,
    )
