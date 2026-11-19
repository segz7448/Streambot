"""Duplicate detection: exact content hashes and perceptual frame hashes.

aHash computed from tiny grayscale frames extracted with ffmpeg - stdlib
only, no imaging dependencies on AGT.
"""
from __future__ import annotations

import hashlib
import subprocess


def content_hash(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def frame_ahash(ffmpeg: str, path: str, at_seconds: float = 1.0,
                size: int = 8) -> int:
    """Average-hash of one downscaled grayscale frame as an int bitmask."""
    proc = subprocess.run(
        [ffmpeg, "-v", "error", "-ss", str(at_seconds), "-i", path,
         "-frames:v", "1", "-vf", f"scale={size}:{size},format=gray",
         "-f", "rawvideo", "-"],
        capture_output=True, timeout=60)
    data = proc.stdout
    if len(data) < size * size:
        raise RuntimeError(f"frame extraction failed for {path}")
    pixels = data[:size * size]
    avg = sum(pixels) / len(pixels)
    bits = 0
    for i, p in enumerate(pixels):
        if p > avg:
            bits |= 1 << i
    return bits


def hamming(a: int, b: int) -> int:
    return bin(a ^ b).count("1")


def is_duplicate(ffmpeg: str, path_a: str, path_b: str,
                 max_hamming: int = 6) -> bool:
    if content_hash(path_a) == content_hash(path_b):
        return True
    ha = frame_ahash(ffmpeg, path_a)
    hb = frame_ahash(ffmpeg, path_b)
    return hamming(ha, hb) <= max_hamming
