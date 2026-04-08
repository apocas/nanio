"""Single source of truth for on-disk path layout.

Layout:

    <data_dir>/<bucket>/                       — bucket root, contains object files
    <data_dir>/<bucket>/<key>                  — raw object bytes
    <data_dir>/<bucket>/.nanio-meta/<key>.json — sidecar metadata
    <data_dir>/.nanio/multipart/<uploadId>/    — in-progress multipart upload state

Every other module asks this module where things go. Centralizing this
means changing the layout (e.g. introducing sharding in v0.2) is a
single-file change.
"""

from __future__ import annotations

import contextlib
import os
from collections.abc import Iterator
from pathlib import Path

from nanio.keys import safe_join

META_DIR_NAME = ".nanio-meta"
MULTIPART_ROOT_NAME = ".nanio"
MULTIPART_SUBDIR = "multipart"


@contextlib.contextmanager
def atomic_write(path: Path, *, text: bool = False) -> Iterator[int]:
    """Yield an `os.open` fd pointing at a fresh tmp file for `path`.

    On context exit, the tmp file is `fsync`ed and atomically `os.replace`d
    into `path`. On exception, the tmp file is unlinked and the exception
    propagates. Used by both the JSON sidecar writer and the multipart
    `init.json` writer so that neither can leave a partially-written file
    on disk after a crash.

    Callers that want a text-mode Python file can set `text=True` and
    wrap the yielded fd with `os.fdopen(fd, "w", encoding="utf-8", closefd=False)`.
    Otherwise they use the fd directly with `os.write` / `os.fdopen(..., "wb")`.

    The tmp file is created with `O_EXCL | O_NOFOLLOW`, so a pre-existing
    file or symlink at the tmp path fails the open — our writer always
    owns the inode it just created.
    """
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    fd = os.open(
        tmp_path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
        0o644,
    )
    try:
        yield fd
        os.fsync(fd)
    except BaseException:
        os.close(fd)
        with contextlib.suppress(FileNotFoundError):
            os.unlink(tmp_path)
        raise
    else:
        os.close(fd)
        os.replace(tmp_path, path)


def assert_inside_data_dir(data_dir: Path, target: Path) -> None:
    """Verify that `target`, after resolving symlinks, stays inside `data_dir`.

    This is the second line of defense against symlink attacks (security
    audit finding H5). The first line is `O_NOFOLLOW` on every `os.open`,
    which catches symlinks at the leaf. This catches symlinks at any
    intermediate component by realpath-ing the whole tree and comparing.

    Raises `PermissionError` if `target` resolves to anything outside
    `data_dir`. The caller should translate that into an `AccessDenied`
    S3 error if appropriate.
    """
    data_real = os.path.realpath(data_dir)
    target_real = os.path.realpath(target)
    # `commonpath` works for both — and is portable.
    try:
        common = os.path.commonpath([data_real, target_real])
    except ValueError:
        # Different drives on Windows; we don't support Windows but be safe.
        raise PermissionError(f"path escape: {target} resolves outside {data_dir}") from None
    if common != data_real:
        raise PermissionError(f"path escape: {target} resolves outside {data_dir}")


def bucket_dir(data_dir: Path, bucket: str) -> Path:
    return data_dir / bucket


def object_path(data_dir: Path, bucket: str, key: str) -> Path:
    return safe_join(data_dir, bucket, key)


def metadata_path(data_dir: Path, bucket: str, key: str) -> Path:
    return safe_join(data_dir, bucket, META_DIR_NAME, key + ".json")


def metadata_root(data_dir: Path, bucket: str) -> Path:
    return data_dir / bucket / META_DIR_NAME


def multipart_root(data_dir: Path) -> Path:
    return data_dir / MULTIPART_ROOT_NAME / MULTIPART_SUBDIR


def multipart_dir(data_dir: Path, upload_id: str) -> Path:
    if "/" in upload_id or ".." in upload_id:
        raise ValueError(f"invalid uploadId: {upload_id!r}")
    return multipart_root(data_dir) / upload_id


def multipart_init_path(data_dir: Path, upload_id: str) -> Path:
    return multipart_dir(data_dir, upload_id) / "init.json"


def multipart_part_path(data_dir: Path, upload_id: str, part_number: int) -> Path:
    if part_number < 1 or part_number > 10_000:
        raise ValueError(f"invalid part number: {part_number}")
    return multipart_dir(data_dir, upload_id) / "parts" / f"{part_number:06d}.bin"


def multipart_part_md5_path(data_dir: Path, upload_id: str, part_number: int) -> Path:
    if part_number < 1 or part_number > 10_000:
        raise ValueError(f"invalid part number: {part_number}")
    return multipart_dir(data_dir, upload_id) / "parts" / f"{part_number:06d}.md5"


def is_internal_name(name: str) -> bool:
    """True if `name` is part of nanio's internal layout (skip in listings)."""
    return name.startswith(".nanio")
