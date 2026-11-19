"""Phase 014: relay - distribute delivered clips to authorized destinations.

A destination is a Telegram chat (channel/group) where the bot has been
added as an administrator by the owner. Authorization model: the paired
owner registers destinations explicitly; the relay never sends anywhere
the owner did not configure. Continuous live-feed restreaming (RTMP) is
out of scope: it requires a capture route that does not officially exist
for arbitrary channels (see app.ingestion.twitch_clips).
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path

from ..core.logging import get_logger

log = get_logger("distribution.relay")


class DestinationStore:
    """0600 JSON file in the state dir; outside the repo."""

    def __init__(self, path: str):
        self.path = Path(path)

    def _read(self) -> list[dict]:
        try:
            return json.loads(self.path.read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            return []

    def _write(self, rows: list[dict]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as fh:
            json.dump(rows, fh)

    def add(self, chat_id: int, label: str) -> None:
        rows = [r for r in self._read() if r["chat_id"] != chat_id]
        rows.append({"chat_id": int(chat_id), "label": label,
                     "added_at": time.time()})
        self._write(rows)

    def remove(self, chat_id: int) -> bool:
        rows = self._read()
        kept = [r for r in rows if r["chat_id"] != chat_id]
        self._write(kept)
        return len(kept) != len(rows)

    def list(self) -> list[dict]:
        return self._read()


class RestreamManager:
    """Owns restream process lifecycle so /stoprelay can stop what
    /relay started. PIDs are tracked in a 0600 state file so a restart
    can still find and stop orphan processes."""

    def __init__(self, state_file: str):
        self.state_file = Path(state_file)
        self._procs: dict[int, object] = {}  # pid -> Popen, this session

    def _alive(self, pid: int) -> bool:
        proc = self._procs.get(pid)
        if proc is not None:
            return proc.poll() is None  # reaps zombies of our children
        # not our child (e.g. after a service restart): reap if we are
        # the parent, otherwise fall back to existence
        try:
            reaped, _ = os.waitpid(pid, os.WNOHANG)
            if reaped == pid:
                return False
        except ChildProcessError:
            pass
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False

    def _pids(self) -> list[int]:
        try:
            return [int(x) for x in self.state_file.read_text().split()]
        except (FileNotFoundError, ValueError):
            return []

    def _save(self, pids: list[int]) -> None:
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.state_file,
                     os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as fh:
            fh.write("\n".join(str(p) for p in pids))

    def active(self) -> list[int]:
        alive = [pid for pid in self._pids() if self._alive(pid)]
        self._save(alive)
        return alive

    def start(self, cmds: list[list[str]]) -> list[int]:
        import subprocess
        pids = self.active()
        for cmd in cmds:
            proc = subprocess.Popen(
                cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self._procs[proc.pid] = proc
            pids.append(proc.pid)
        self._save(pids)
        log.info("restream started: %d process(es)", len(cmds))
        return pids

    def stop(self) -> int:
        import signal
        import time
        stopped = 0
        for pid in self.active():
            try:
                os.kill(pid, signal.SIGTERM)
                stopped += 1
            except OSError:
                pass
        deadline = time.time() + 5
        for pid, proc in list(self._procs.items()):
            remaining = max(0.0, deadline - time.time())
            try:
                proc.wait(timeout=remaining)  # reaps; ffmpeg finalizes
            except Exception:
                pass
        self._procs.clear()
        self._save([])
        log.info("restream stopped: %d process(es)", stopped)
        return stopped


class Relayer:
    def __init__(self, store: DestinationStore, send_video):
        """send_video(chat_id, path, caption) -> message_id; injected so
        the transport stays the maintained framework and tests stay
        local."""
        self.store = store
        self.send_video = send_video

    def restream(self, feed_url: str, rtmp_destinations: list[str],
                 ffmpeg_bin: str = "ffmpeg") -> list[str]:
        """RTMP restream of an EXPLICITLY owner-provided feed (a source
        the owner supplies and is authorized to restream, e.g. their own
        channel output) to owner-configured RTMP endpoints. This is the
        requested RTMP scope; it is separate from Telegram clip
        forwarding and needs no Twitch capture route because the feed
        comes from the owner. Returns one launched process command per
        destination; the caller owns process lifecycle and stop control
        (/stoprelay). Never invent a feed: no input, no restream."""
        if not feed_url:
            raise ValueError("no owner-provided feed configured")
        if not rtmp_destinations:
            raise ValueError("no RTMP destinations configured")
        cmds = []
        for dest in rtmp_destinations:
            cmds.append([
                ffmpeg_bin, "-re", "-i", feed_url,
                "-c", "copy", "-f", "flv", dest,
            ])
        log.info("restream prepared for %d destination(s)", len(cmds))
        return cmds

    def relay_clip(self, video_path: str, caption: str = "") -> dict:
        """Send one clip to every registered destination. Returns
        {chat_id: message_id-or-error} - partial failures are visible,
        never silently dropped."""
        results: dict[int, object] = {}
        for dest in self.store.list():
            chat_id = int(dest["chat_id"])
            try:
                results[chat_id] = self.send_video(
                    chat_id, video_path, caption)
            except Exception as exc:
                log.warning("relay to %s failed: %s", chat_id, exc)
                results[chat_id] = f"FAILED: {exc.__class__.__name__}"
        return results
