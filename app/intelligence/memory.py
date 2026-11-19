"""Persistent agent memory for NOVA Clip Engine.

Not settings: a growing memory of usernames, channels, owner
preferences, past clips and observed patterns, each entry carrying
provenance (who said it / where it was learned) so recall can cite its
basis and stale entries can be re-verified. 0600 JSON in the state dir;
never in the repository. This is memory, not intelligence: it records
and recalls; it does not claim understanding.
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path

KINDS = ("username", "channel", "preference", "clip", "pattern",
         "publishing_rule")


class Memory:
    def __init__(self, path: str):
        self.path = Path(path)

    def _read(self) -> list[dict]:
        try:
            return json.loads(self.path.read_text())
        except (FileNotFoundError, json.JSONDecodeError, ValueError):
            return []

    def _write(self, rows: list[dict]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as fh:
            json.dump(rows, fh)

    def remember(self, kind: str, key: str, value: str, source: str,
                 confidence: str = "stated") -> dict:
        """Record or update a fact. source is its provenance (e.g.
        'owner iMessage 2026-11-19'); confidence: stated (owner said
        it), observed (seen in data), inferred (derived - weakest)."""
        if kind not in KINDS:
            raise ValueError(f"unknown memory kind {kind!r}")
        if confidence not in ("stated", "observed", "inferred"):
            raise ValueError(f"unknown confidence {confidence!r}")
        rows = [r for r in self._read()
                if not (r["kind"] == kind and r["key"] == key)]
        entry = {"kind": kind, "key": key, "value": value,
                 "source": source, "confidence": confidence,
                 "updated_at": time.time()}
        rows.append(entry)
        self._write(rows)
        return entry

    def recall(self, kind: str | None = None, key_contains: str = ""
               ) -> list[dict]:
        rows = self._read()
        if kind:
            rows = [r for r in rows if r["kind"] == kind]
        if key_contains:
            rows = [r for r in rows if key_contains.lower()
                    in r["key"].lower()]
        return sorted(rows, key=lambda r: r["updated_at"], reverse=True)

    def get(self, kind: str, key: str) -> dict | None:
        for r in self._read():
            if r["kind"] == kind and r["key"] == key:
                return r
        return None

    def forget(self, kind: str, key: str) -> bool:
        rows = self._read()
        kept = [r for r in rows
                if not (r["kind"] == kind and r["key"] == key)]
        self._write(kept)
        return len(kept) != len(rows)
