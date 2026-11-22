"""Sealed clip inputs: unique never-reused paths, durable completion
evidence, and private recovery snapshots.

A clip row becomes recovery-eligible only when its raw input carries
durable sealed evidence: the producer fsyncs the finished file and its
directory, records sha256 + size, and commits that evidence WITH the
PENDING row in one transaction, after the producing child has exited.
A published raw is never reopened for appending.

Recovery never consumes the mutable source file: it takes a PRIVATE
SNAPSHOT and verifies the snapshot's digest against the producer's
pre-existing committed digest. The stored digest is the evidence; the
snapshot hash is only the comparison. A mismatch fences the clip.
"""
from __future__ import annotations

import hashlib
import os
import shutil
import uuid
from pathlib import Path


def unique_path(directory, prefix: str, suffix: str) -> Path:
    """A path in `directory` that has never been used by any run."""
    return Path(directory) / f"{prefix}-{uuid.uuid4().hex[:12]}{suffix}"


def seal_file(path) -> tuple[str, int]:
    """Fsync the finished file and its directory, then return
    (sha256, size). Call only after the producing child has EXITED.
    Raises on any failure: an unsealable file must not be published."""
    path = Path(path)
    fd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(fd)
        size = os.fstat(fd).st_size
    finally:
        os.close(fd)
    dfd = os.open(str(path.parent), os.O_RDONLY)
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest(), size


def take_verified_snapshot(src, snapshot_dir, expected_sha256: str,
                           expected_bytes: int):
    """Copy the sealed source into a private snapshot path and return it
    only when the SNAPSHOT matches the producer's pre-existing committed
    digest+size. Returns None on any mismatch or failure; never modifies
    or deletes the source."""
    src = Path(src)
    snap = unique_path(snapshot_dir, "snapshot", src.suffix or ".bin")
    Path(snapshot_dir).mkdir(parents=True, exist_ok=True)
    try:
        if not src.exists() or src.stat().st_size != expected_bytes:
            return None
        shutil.copyfile(src, snap)
        fd = os.open(str(snap), os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        h = hashlib.sha256()
        with open(snap, "rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                h.update(chunk)
        if h.hexdigest() != expected_sha256:
            snap.unlink(missing_ok=True)
            return None
        return snap
    except Exception:
        snap.unlink(missing_ok=True)
        return None
