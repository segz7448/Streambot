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
    if size == 0:
        raise ValueError("empty output is unsealable")
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


def reserve_output(directory, prefix: str, suffix: str) -> Path:
    """Reserve a per-attempt exclusive output path: create the file
    O_CREAT|O_EXCL so this attempt OWNS it before any writer is
    spawned. A random name alone is not a reservation. Raises if no
    exclusive path can be reserved."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    for _ in range(100):
        cand = directory / f"{prefix}-{uuid.uuid4().hex[:12]}{suffix}"
        try:
            fd = os.open(str(cand), os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                         0o600)
        except FileExistsError:
            continue
        os.close(fd)
        return cand
    raise RuntimeError(f"could not reserve exclusive output in {directory}")


def reserve_exact_or_unique(path) -> Path:
    """Take exclusive ownership of exactly `path` (O_CREAT|O_EXCL); if
    it already exists, reserve a unique sibling derived from its stem
    instead. Never returns a path another writer could still own."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                     0o600)
        os.close(fd)
        return path
    except FileExistsError:
        return reserve_output(path.parent, path.stem, path.suffix)


def fsync_tree(path) -> None:
    """Fsync `path` and its containing directory. Raises on failure:
    durability evidence must be real before any durable commit."""
    path = Path(path)
    fd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    dfd = os.open(str(path.parent), os.O_RDONLY)
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)


def take_verified_snapshot_bounded(src, snapshot_dir, expected_sha256: str,
                                   expected_bytes: int,
                                   max_seconds: float = 120.0):
    """Bounded private snapshot: copies at most expected_bytes from the
    source, aborts if the source delivers MORE than the sealed size or
    the copy exceeds the time cap, verifies copied size AND digest
    against the producer's pre-existing committed evidence, fsyncs the
    snapshot file and directory, and unlinks the snapshot on every
    failure path. Returns the snapshot path or None."""
    import time as _time
    src = Path(src)
    Path(snapshot_dir).mkdir(parents=True, exist_ok=True)
    snap = unique_path(snapshot_dir, "snapshot", src.suffix or ".bin")
    deadline = _time.monotonic() + max_seconds
    try:
        try:
            st = src.stat()
        except OSError:
            return None
        if st.st_size != expected_bytes:
            return None
        copied = 0
        h = hashlib.sha256()
        with open(src, "rb") as fin, open(snap, "wb") as fout:
            while copied < expected_bytes:
                if _time.monotonic() > deadline:
                    raise TimeoutError("snapshot copy exceeded time cap")
                chunk = fin.read(min(1 << 20, expected_bytes - copied))
                if not chunk:
                    break
                h.update(chunk)
                fout.write(chunk)
                copied += len(chunk)
            if fin.read(1):
                # Source grew past the sealed size: do not trust it.
                raise ValueError("source larger than sealed bytes")
            fout.flush()
            os.fsync(fout.fileno())
        if copied != expected_bytes or h.hexdigest() != expected_sha256:
            snap.unlink(missing_ok=True)
            return None
        dfd = os.open(str(Path(snapshot_dir)), os.O_RDONLY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
        return snap
    except Exception:
        snap.unlink(missing_ok=True)
        return None
