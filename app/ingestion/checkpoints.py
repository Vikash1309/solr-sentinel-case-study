"""File identity + byte-offset checkpoints (doc §4.1).

A checkpoint tracks *file identity* (device/inode + a fingerprint of the first bytes), not just the
file name, so log rotation (file replaced) and truncation are detected and the new file is read from
the start instead of silently skipping or re-reading data."""
from __future__ import annotations

import hashlib
import os
from pathlib import Path

from ..storage.repository import Repository


def file_identity(path: Path) -> str:
    st = os.stat(path)
    try:
        with open(path, "rb") as f:
            head = hashlib.sha1(f.read(256), usedforsecurity=False).hexdigest()[:12]
    except OSError:
        head = "na"
    return f"{st.st_dev}:{st.st_ino}:{head}"


def resume_offset(repo: Repository, path: Path) -> tuple[str, int]:
    """Return (current file identity, offset to resume from)."""
    key = str(path.resolve())
    fid = file_identity(path)
    old_fid, offset = repo.checkpoint(key)
    size = path.stat().st_size
    # the head fingerprint covers only the first 256 bytes; compare it once the file is that large
    same = old_fid is not None and (old_fid == fid or (old_fid.rsplit(":", 1)[0] == fid.rsplit(":", 1)[0] and offset < 256))
    if not same or size < offset:
        offset = 0  # new file, rotated or truncated
    return fid, offset


def commit(repo: Repository, path: Path, fid: str, offset: int):
    repo.set_checkpoint(str(path.resolve()), fid, offset)
