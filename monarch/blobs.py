"""Blob stores: the bytes behind blob-referencing columns (path: {blob: filestore}).
Copying is idempotent: an existing key is never rewritten. Deletion follows the store's
manifest `eviction` declaration: `keep` stores are never touched here (keys may be shared
across tenants; the owning service's GC/TTL reclaims), `delete` stores lose the org's
objects per key at eviction (cell_eviction.py)."""

import os
import shutil
from collections.abc import Callable
from dataclasses import dataclass

from .membership import BlobMembership


@dataclass(frozen=True)
class Bucket:
    """One cell's blob bucket. The demo backs it with a local directory (fleet.yaml file_path)."""

    root: str

    def path(self, key: str) -> str:
        return os.path.join(self.root, key)


def copy_blob(src: Bucket, dst: Bucket, key: str) -> bool:
    """Copy one blob; False if the sink already had it."""
    if os.path.exists(dst.path(key)):
        return False
    os.makedirs(os.path.dirname(dst.path(key)), exist_ok=True)
    shutil.copyfile(src.path(key), dst.path(key))
    return True


def copy_pending(members: BlobMembership, copier: Callable[[str], bool], limit: int) -> int:
    """Copy up to `limit` uncopied keys and mark them -- the convergence step the stream
    loop interleaves. A crash between copy and mark re-runs the copy, which skips."""
    keys = members.uncopied(limit)
    for key in keys:
        copier(key)
        members.mark_copied(key)
    members.flush()
    return len(keys)


def delete_blob(bucket: Bucket, key: str) -> bool:
    """Delete one blob; False if it was already gone."""
    try:
        os.remove(bucket.path(key))
        return True
    except FileNotFoundError:
        return False
