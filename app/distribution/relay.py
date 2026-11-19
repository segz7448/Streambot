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
import re
import time
from pathlib import Path

from ..core.logging import get_logger

_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.\-]*:")
_CANONICAL_KEYLESS = re.compile(r"^rtmps?://[^/?#@\s]+/?$", re.IGNORECASE)


def validate_feed(feed: str, input_root: Path) -> str:
    """A restream feed is either a credential-free http(s) URL or a
    local file under the authorized input root. Anything else - other
    protocols, userinfo, query/fragment credentials, flag-shaped
    strings, symlink escapes - is refused. Returns the safe specifier.
    """
    f = feed.strip()
    if not f or f.startswith("-"):
        raise ValueError("feed is empty or flag-shaped")
    if _SCHEME.match(f):
        from urllib.parse import urlparse
        try:
            p = urlparse(f)
        except ValueError:
            raise ValueError("feed URL unparseable")
        if p.scheme.lower() not in ("http", "https"):
            raise ValueError("only http(s) feeds are supported")
        if p.username or p.password or p.query or p.fragment:
            raise ValueError("feed URL carries credentials/parameters")
        if not p.hostname:
            raise ValueError("feed URL has no host")
        return f
    root = input_root.resolve()
    p = (Path(f).resolve() if os.path.isabs(f) else (root / f).resolve())
    if p != root and root not in p.parents:
        raise ValueError("local feed escapes the authorized input root")
    return str(p)


def validate_output(dest: str, output_root: Path) -> str:
    """A destination is either a canonical keyless rtmp(s) endpoint or
    a local file resolved UNDER the dedicated output root (no absolute
    paths, no '..' or symlink escape, nothing flag-shaped). Anything
    else is refused. Returns the safe specifier."""
    d = dest.strip()
    if not d or d.startswith("-"):
        raise ValueError("destination is empty or flag-shaped")
    if _CANONICAL_KEYLESS.match(d):
        return d
    if _SCHEME.match(d) or "://" in d or "@" in d:
        raise ValueError("destination carries a scheme or credentials")
    root = output_root.resolve()
    p = (root / d).resolve()  # relative to the output root only
    if p != root and root not in p.parents:
        raise ValueError("local output escapes the output root")
    return str(p)

log = get_logger("distribution.relay")


def secret_in_rtmp_url(url: str) -> bool:
    """True when a destination is anything but a canonical keyless
    rtmp(s) endpoint or a plain local file path. Stream keys must not
    enter process arguments on a shared host. Fail closed: userinfo,
    non-trivial path, query or fragment refuse; every scheme-like
    prefix other than rtmp(s) refuses (srt, pipe, tcp, udp, concat and
    friends are ffmpeg protocols, not files), including slash-less and
    single-slash forms (rtmp:host, rtmp:/host) which ffmpeg parses as
    network protocols even though they lack '//'; unparseable input
    refuses. Local paths carry no credential channel and return False -
    confining them to the state dir is the caller's job (relay_reply)."""
    import re
    stripped = url.strip()
    if re.match(r"^rtmps?://[^/?#@\s]+/?$", stripped, re.IGNORECASE):
        return False  # canonical keyless endpoint: scheme://host[/]
    if re.match(r"^[A-Za-z][A-Za-z0-9+.\-]*:", stripped):
        return True  # any other scheme-like prefix, any form
    if "://" in stripped or "@" in stripped:
        return True
    return False  # local file output


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
    /relay started. Processes are tracked in a 0600 state file WITH
    THEIR IDENTITY (pid + /proc start time + cmdline): a bare pid is
    never enough to signal, because pid reuse after a reboot could
    make /stoprelay hit an unrelated same-UID workload on this shared
    host. A record whose identity cannot be verified is reported stale
    and left alone - never signaled."""

    def __init__(self, state_file: str):
        self.state_file = Path(state_file)
        self._procs: dict[int, object] = {}  # pid -> Popen, this session

    @staticmethod
    def _identity(pid: int) -> dict | None:
        """Current /proc identity of a pid: start time + cmdline."""
        try:
            with open(f"/proc/{pid}/stat") as fh:
                start = fh.read().rsplit(")", 1)[1].split()[19]
            with open(f"/proc/{pid}/cmdline", "rb") as fh:
                cmd = fh.read().replace(b"\0", b" ").decode(
                    errors="replace").strip()
            return {"start": start, "cmd": cmd[:200]}
        except (OSError, IndexError):
            return None

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

    def _records(self) -> list[dict]:
        try:
            lines = self.state_file.read_text().splitlines()
        except FileNotFoundError:
            return []
        records = []
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                records.append({"pid": int(rec["pid"]),
                                "start": rec.get("start"),
                                "cmd": rec.get("cmd", "")})
            except (ValueError, KeyError, TypeError):
                try:  # legacy format: bare pid, identity unknown
                    records.append({"pid": int(line), "start": None,
                                    "cmd": ""})
                except ValueError:
                    continue
        return records

    def _save(self, records: list[dict]) -> None:
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.state_file,
                     os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as fh:
            for rec in records:
                fh.write(json.dumps(rec) + "\n")

    def active(self) -> list[int]:
        alive = [rec for rec in self._records() if self._alive(rec["pid"])]
        self._save(alive)
        return [rec["pid"] for rec in alive]

    def start(self, cmds: list[list[str]]) -> list[int]:
        import subprocess
        records = [rec for rec in self._records()
                   if self._alive(rec["pid"])]
        for cmd in cmds:
            proc = subprocess.Popen(
                cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self._procs[proc.pid] = proc
            ident = self._identity(proc.pid) or {"start": None, "cmd": ""}
            records.append({"pid": proc.pid, "start": ident["start"],
                            "cmd": ident["cmd"]})
        self._save(records)
        log.info("restream started: %d process(es)", len(cmds))
        return [rec["pid"] for rec in records]

    def stop(self) -> int:
        """SIGTERM only processes whose identity verifies: our own live
        children, or a state-file record whose /proc start time still
        matches AND whose cmdline is still ffmpeg. Anything else is a
        possible pid-reuse victim - reported stale, never signaled."""
        import signal
        import time
        stopped = stale = 0
        for rec in self._records():
            pid = rec["pid"]
            if not self._alive(pid):
                continue
            proc = self._procs.get(pid)
            ours = proc is not None and proc.poll() is None
            verified = False
            if not ours and rec.get("start"):
                cur = self._identity(pid)
                verified = bool(cur and cur["start"] == rec["start"]
                                and cur["cmd"] == rec["cmd"])
            if ours or verified:
                try:
                    os.kill(pid, signal.SIGTERM)
                    stopped += 1
                except OSError:
                    pass
            else:
                stale += 1
                log.warning("restream pid %s identity unverifiable - "
                            "NOT signaled (possible pid reuse)", pid)
        deadline = time.time() + 5
        for pid, proc in list(self._procs.items()):
            remaining = max(0.0, deadline - time.time())
            try:
                proc.wait(timeout=remaining)  # reaps; ffmpeg finalizes
            except Exception:
                pass
        self._procs.clear()
        self._save([])
        log.info("restream stopped: %d signaled, %d stale-unverified",
                 stopped, stale)
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
        for dest in rtmp_destinations:
            if secret_in_rtmp_url(dest):
                raise ValueError(
                    "secret-bearing RTMP destination refused: stream "
                    "keys must not appear in process arguments")
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
