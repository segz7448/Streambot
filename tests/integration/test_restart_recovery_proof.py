"""Restart recovery proof with a REAL uncatchable kill (SIGKILL).

A victim subprocess runs the real capture chain (PlaybackWorker/hls_pump
against an in-memory opener, real buffer, real detection, real sealed
extraction). The parent then kills it -9 with no cleanup opportunity:

V1 (kill after a sealed PENDING clip exists, before any finish):
   boot recover_interrupted_sessions marks the session INTERRUPTED;
   finalize_interrupted_sessions (the owner-invoked recovery entry)
   moves the sealed clip to HELD exactly once, from a verified private
   snapshot, with ZERO admission jobs, the raw sealed input untouched,
   and a second recovery run changing nothing.

V2 (kill DURING the finish edit child):
   the claimed (RECOVERING) clip stays fenced - recovery never reclaims,
   never resets and never publishes it; the orphaned edit's output is
   never adopted.

All media and state are disposable and generated; no network, source,
device or real artifact is involved.
"""
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

REPO = Path(__file__).resolve().parents[2]

VICTIM = r'''
import asyncio, hashlib, os, sys, time
from pathlib import Path
sys.path.insert(0, {repo!r})
state_dir = Path(sys.argv[1]); mode = sys.argv[2]
root = state_dir / "victim-gen"; root.mkdir(parents=True, exist_ok=True)
import subprocess
from app.core import content_gate as cg
from app.core.database import connect, migrate
from app.bot import source_auth
from app.ingestion import twitch_playback as tp
from app.recording.buffer import RollingBuffer
from app.recording.playback_worker import PlaybackWorker

def make_segment(path, loud, seconds=2.0):
    vol = "28dB" if loud else "-8dB"
    subprocess.run(["ffmpeg","-hide_banner","-loglevel","error",
        "-f","lavfi","-i","testsrc=size=640x360:rate=15",
        "-f","lavfi","-i","sine=frequency=440","-af",f"volume={vol}",
        "-t",str(seconds),"-c:v","libx264","-g","15","-pix_fmt","yuv420p",
        "-c:a","aac","-f","mpegts",str(path)], check=True)
    return path.read_bytes()

quiet = make_segment(root/"q.ts", False); loud = make_segment(root/"l.ts", True)
segments = [quiet, quiet, loud, quiet]

def opener(req, timeout, limit, deadline, should_stop):
    url = req.full_url
    if url.endswith("v.m3u8"):
        lines = ["#EXTM3U","#EXT-X-TARGETDURATION:2","#EXT-X-MEDIA-SEQUENCE:0"]
        for i in range(len(segments)):
            lines += ["#EXTINF:2.0,", f"seg{i}.ts"]
        lines.append("#EXT-X-ENDLIST")
        return 200, "\n".join(lines).encode()
    return 200, segments[int(url.rsplit("seg",1)[1].split(".")[0])]

def resolved():
    v = tp.Variant("360p", 360, 1_000_000, "https://fake/v.m3u8")
    return tp.Resolved("chan", v, (v,), time.time())

db_path = str(state_dir/"nova.db")
conn = connect(db_path); migrate(conn)
conn.execute("INSERT INTO twitch_sources (username, created_at, updated_at)"
             " VALUES ('chan', 0, 0)")
cur = conn.execute("INSERT INTO stream_sessions (source_id, state,"
                   " started_at, ingest_mode, notes) VALUES (1,'RECORDING',"
                   " ?, 'playback', '{}')", (time.time(),))
sid = cur.lastrowid; conn.commit(); conn.close()
source_auth.set_scope(state_dir, "chan", "record", True)
cg.set_provider(lambda u: cg.Evidence(u, "GTA 6 live", time.time(), True))

work = state_dir/"sessions"/"chan"; work.mkdir(parents=True, exist_ok=True)
(state_dir/"runtime").mkdir(exist_ok=True)
buf = RollingBuffer(str(work), session_id=sid, segment_seconds=2,
                    pre_event_seconds=2, post_event_seconds=2,
                    max_storage_mb=64)
worker = PlaybackWorker(buf, "chan", db_path, state_dir, "ffmpeg",
                        resolve_fn=lambda channel, **k: resolved(),
                        opener=opener, host_ok=lambda h: True,
                        auth_poll_seconds=0.0, ffprobe="ffprobe")

class Ctx:
    stopping = False
    def heartbeat(self): pass

asyncio.run(worker.run(Ctx()))
print("CAPTURE_DONE", worker.end_reason, "segments", len(buf.segments()),
      flush=True)

from app.detection.detector import CandidateDetector
from app.clipping.auto import AutoClipper
from app.intelligence.providers import HeuristicProvider
CandidateDetector("ffmpeg", buf, db_path).process_new_segments(True)
made = AutoClipper(buf, db_path, HeuristicProvider(), "ffmpeg", str(work),
                   pre_seconds=2, post_seconds=2).process_pending()
conn = connect(db_path)
row = conn.execute("SELECT id, file_path, sealed_sha256 FROM clips"
                   " WHERE session_id=? AND status='PENDING'",
                   (sid,)).fetchone()
conn.close()
cid, path, sha = row
real = hashlib.sha256(Path(path).read_bytes()).hexdigest()
assert real == sha, "seal digest mismatch before kill"
print("CLIP_SEALED", cid, path, sha, flush=True)

if mode == "kill_in_edit":
    from app.pipeline.session import finish_pending_clips
    def suicide(pid, pgid, token):
        print("EDIT_SPAWNED", pid, flush=True)
        os.kill(os.getpid(), 9)
        return True
    conn = connect(db_path)
    finish_pending_clips(conn, state_dir=state_dir, session_id=sid,
                         source_label="chan", out_dir=work/"edited",
                         runtime_dir=state_dir/"runtime", ffmpeg="ffmpeg",
                         ffprobe="ffprobe", auto_deliver=False,
                         on_spawn=suicide)
    print("FINISH_RETURNED_UNEXPECTEDLY", flush=True)
else:
    print("IDLE", flush=True)
    time.sleep(3600)
'''


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class RestartRecoveryProof(unittest.TestCase):
    def setUp(self):
        self.td = TemporaryDirectory()
        self.addCleanup(self.td.cleanup)
        self.sd = Path(self.td.name) / "state"
        self.sd.mkdir()
        self.victim = Path(self.td.name) / "victim.py"
        self.victim.write_text(VICTIM.replace("{repo!r}", repr(str(REPO))))

    def _run_victim(self, mode: str, kill_after: str):
        env = dict(os.environ, PYTHONPATH=str(REPO))
        proc = subprocess.Popen(
            [sys.executable, str(self.victim), str(self.sd), mode],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            env=env)
        out, marker = [], None
        deadline = time.monotonic() + 90
        for line in proc.stdout:
            out.append(line)
            if kill_after in line:
                marker = line.strip()
                proc.kill()          # SIGKILL: no cleanup opportunity
                break
            if time.monotonic() > deadline:
                proc.kill()
                proc.stdout.close()
                self.fail(f"victim did not reach {kill_after}: {out}")
        proc.wait(timeout=10)
        rest = proc.stdout.read()
        if rest:
            out.append(rest)
        proc.stdout.close()
        self.assertEqual(proc.returncode, -9, "".join(out))
        return "".join(out), marker

    def _db(self):
        return sqlite3.connect(str(self.sd / "nova.db"))

    def test_kill_after_seal_recovers_held_once(self):
        out, marker = self._run_victim("capture", "CLIP_SEALED")
        _, cid, raw_path, sha = marker.split()
        cid = int(cid)
        self.assertIn("CAPTURE_DONE ended", out)

        from app.pipeline.session import (recover_interrupted_sessions,
                                          finalize_interrupted_sessions)
        # boot: the dead process's session is marked INTERRUPTED, once
        self.assertEqual(recover_interrupted_sessions(self.sd), 1)
        self.assertEqual(recover_interrupted_sessions(self.sd), 0)
        conn = self._db()
        state = conn.execute("SELECT state FROM stream_sessions"
                             ).fetchone()[0]
        self.assertEqual(state, "INTERRUPTED")
        row = conn.execute("SELECT status, sealed_sha256 FROM clips"
                           " WHERE id=?", (cid,)).fetchone()
        self.assertEqual(row, ("PENDING", sha))
        events_before = conn.execute(
            "SELECT COUNT(*) FROM candidate_events").fetchone()[0]
        conn.close()
        self.assertEqual(_sha(Path(raw_path)), sha)   # seal survives kill

        # owner-invoked recovery: sealed clip -> HELD, exactly once
        self.assertEqual(
            finalize_interrupted_sessions(self.sd, "ffmpeg", "ffprobe"), 1)
        conn = self._db()
        status, qc_json, edited = conn.execute(
            "SELECT status, qc_report_json, file_path FROM clips"
            " WHERE id=?", (cid,)).fetchone()
        self.assertEqual(status, "HELD")
        self.assertTrue(json.loads(qc_json)["qc_passed"])
        self.assertTrue(Path(edited).exists())
        # zero side effects: no admission jobs, no redetection
        jobs = conn.execute("SELECT COUNT(*) FROM job_queue").fetchone()[0]
        self.assertEqual(jobs, 0)
        events_after = conn.execute(
            "SELECT COUNT(*) FROM candidate_events").fetchone()[0]
        self.assertEqual(events_before, events_after)
        conn.close()
        # raw sealed input preserved byte-identical; snapshots cleaned
        self.assertEqual(_sha(Path(raw_path)), sha)
        snaps = list((self.sd / "recovery").glob("**/*"))
        self.assertEqual([p for p in snaps if p.is_file()], [])
        # idempotent: a second recovery changes nothing
        self.assertEqual(
            finalize_interrupted_sessions(self.sd, "ffmpeg", "ffprobe"), 0)
        conn = self._db()
        self.assertEqual(conn.execute(
            "SELECT status FROM clips WHERE id=?", (cid,)).fetchone()[0],
            "HELD")
        conn.close()
        edited_files = list((self.sd / "sessions" / "chan").glob(
            "clip-recovery-*.mp4"))
        self.assertEqual(len(edited_files), 1)
        print("KILL9_RECOVERY: sealed clip survived; HELD once; "
              "zero duplicate side effects", flush=True)

    def test_kill_during_edit_stays_fenced(self):
        out, marker = self._run_victim("kill_in_edit", "EDIT_SPAWNED")
        _, cid, raw_path, sha = next(
            l.strip().split() for l in out.splitlines()
            if l.startswith("CLIP_SEALED"))
        cid = int(cid)
        time.sleep(3)      # let the orphaned edit child finish writing

        from app.pipeline.session import (recover_interrupted_sessions,
                                          finalize_interrupted_sessions)
        self.assertEqual(recover_interrupted_sessions(self.sd), 1)
        held = finalize_interrupted_sessions(self.sd, "ffmpeg", "ffprobe")
        self.assertEqual(held, 0, "recovery must never reclaim a "
                                  "clip claimed by a dead finalizer")
        conn = self._db()
        status, path = conn.execute(
            "SELECT status, file_path FROM clips WHERE id=?",
            (cid,)).fetchone()
        jobs = conn.execute("SELECT COUNT(*) FROM job_queue").fetchone()[0]
        conn.close()
        self.assertNotEqual(status, "HELD")
        self.assertEqual(status, "RECOVERING")   # fenced, not reset
        self.assertEqual(path, raw_path)         # still names raw input
        self.assertEqual(jobs, 0)
        self.assertEqual(_sha(Path(raw_path)), sha)
        # the orphaned edit's output (if written) is never adopted
        stray = list((self.sd / "sessions" / "chan" / "edited").glob(
            "clip-edited-*"))
        for f in stray:
            self.assertNotEqual(f.name, Path(path).name)
        print("KILL9_MID_EDIT: claimed clip fenced, never adopted; "
              f"stray outputs on disk: {len(stray)}", flush=True)


if __name__ == "__main__":
    unittest.main()
