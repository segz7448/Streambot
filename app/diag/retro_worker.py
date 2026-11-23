"""Worker entry for the one-off diagnostic export. Runs INSIDE one
owned child process spawned by the service; every ffmpeg/ffprobe
grandchild lives in this child's process group, so the parent's
registered-group deadline/cancel/kill covers all of them. Writes the
pipeline evidence durably and exits 0 only on a clean run."""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
from pathlib import Path


def _fsync_json(path: Path, obj: dict) -> None:
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w") as fh:
        json.dump(obj, fh, indent=1, sort_keys=True)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    dfd = os.open(str(path.parent), os.O_RDONLY)
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)


def _verify_held_outputs(diag: Path, ffmpeg: str, ffprobe: str) -> list:
    """Exact HELD rows from the ISOLATED diag DB (id + file_path), each
    probed (finite duration, dims, audio+video streams), full-decode QC,
    sha256 and size. Runs inside the owned worker, inside its deadline."""
    import hashlib
    import sqlite3
    from ..qc.verify import check_media, probe
    from ..editing.presets import PRESETS
    db = sqlite3.connect(f"file:{diag / 'nova.db'}?mode=ro", uri=True)
    try:
        rows = db.execute(
            "SELECT id, file_path, preset, duration_seconds"
            " FROM clips WHERE status = 'HELD'"
        ).fetchall()
    finally:
        db.close()
    out = []
    for clip_id, path, preset_name, planned_seconds in rows:
        entry = {"db_clip_id": clip_id, "path": path, "ok": False}
        preset = PRESETS.get(preset_name or "")
        if preset is not None:
            entry["expected_width"] = preset.width
            entry["expected_height"] = preset.height
        if planned_seconds is not None:
            entry["expected_duration"] = float(planned_seconds)
        try:
            real = os.path.realpath(path)
            if not real.startswith(os.path.realpath(str(diag)) + os.sep):
                entry["why"] = "path outside diag dir"
                out.append(entry)
                continue
            meta = probe(ffprobe, path)
            streams = meta.get("streams", [])
            video = next((s for s in streams
                          if s.get("codec_type") == "video"), None)
            audio = next((s for s in streams
                          if s.get("codec_type") == "audio"), None)
            duration = float(meta.get("format", {}).get("duration", 0))
            qc = check_media(ffprobe, ffmpeg, path)
            with open(path, "rb") as fh:
                digest = hashlib.sha256(fh.read()).hexdigest()
            entry.update({
                "duration": duration,
                "width": (video or {}).get("width"),
                "height": (video or {}).get("height"),
                "has_video": video is not None,
                "has_audio": audio is not None,
                "qc_ok": bool(qc.ok),
                "qc_issues": list(getattr(qc, "issues", []) or []),
                "sha256": digest,
                "bytes": os.lstat(path).st_size,
                "ok": (math.isfinite(duration) and duration > 0
                       and video is not None and audio is not None
                       and bool(qc.ok)),
            })
            if not entry["ok"]:
                entry["why"] = "probe/QC failed"
        except Exception as exc:
            entry["why"] = f"{exc.__class__.__name__}: {exc}"
        out.append(entry)
    return out


def main() -> int:
    os.umask(0o077)
    ap = argparse.ArgumentParser()
    ap.add_argument("--state", required=True)
    ap.add_argument("--source", required=True)
    ap.add_argument("--diag", required=True)
    ap.add_argument("--label", required=True)
    args = ap.parse_args()
    os.makedirs(args.diag, mode=0o700, exist_ok=True)
    result_path = Path(args.diag) / "pipeline-result.json"
    from ..clipping.retrospective import process_recording
    from ..editing.editor import edit_clip
    from ..intelligence.memory import Memory
    from ..bot.media import ffmpeg_paths
    from ..bot import restartproof as rp
    ff, fp = ffmpeg_paths()
    # the editor spawns ffmpeg with start_new_session=True: its own
    # session escapes THIS worker's process group. Register every edit
    # child in a SECOND registry (editor-only, no cross-process writers)
    # through the accepted two-phase hooks, exactly like the live path,
    # so proof and cancel cover BOTH groups.
    edit_reg = Path(args.diag) / "writers-edit.json"
    if not rp.init_registry(edit_reg):
        print("editor registry init failed; not running", file=sys.stderr)
        return 2

    def _pre_spawn():
        return rp.record_spawn_intent(edit_reg)

    def _on_spawn(pid, pgid, token):
        return rp.record_spawned(edit_reg, token, pid, pgid)

    def _edit_with_hooks(*a, **kw):
        kw["pre_spawn"] = _pre_spawn
        kw["on_spawn"] = _on_spawn
        return edit_clip(*a, **kw)

    try:
        out = process_recording(
            ff, fp, args.source, str(Path(args.diag) / "work"),
            str(Path(args.diag) / "nova.db"),  # ISOLATED DB, never production
            Memory(str(Path(args.diag) / "memory.json")),
            args.label,
            edit_fn=_edit_with_hooks,
            runtime_dir=str(Path(args.diag) / "runtime"))
        # final probe/QC INSIDE the owned worker, before result and
        # exit: every held output gets the exact HELD DB path (never a
        # glob), a finite-duration probe with dims and audio/video
        # streams, full decode QC, sha256 and size. The controller only
        # reads and re-hashes afterwards.
        if out.get("status") == "held":
            out["outputs"] = _verify_held_outputs(
                Path(args.diag), ff, fp)
            out["outputs_verified"] = bool(out["outputs"]) and all(
                o["ok"] for o in out["outputs"])
        out["worker_completed"] = True
        _fsync_json(result_path, out)
        return 0
    except Exception as exc:
        try:
            _fsync_json(result_path,
                        {"worker_completed": False,
                         "error": f"{exc.__class__.__name__}: {exc}"})
        except Exception:
            pass
        return 2


if __name__ == "__main__":
    sys.exit(main())
