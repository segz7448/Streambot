"""Validated HLS capture: this process downloads, ffmpeg never touches the network.

Every playlist and segment request goes through the same opener and host
check as the resolver (HTTPS only, allowlisted hosts, redirects refused,
bounded sizes). Bytes are piped to ffmpeg on stdin (``-f mpegts -i pipe:0``
with only the ``pipe`` protocol whitelisted), so ffmpeg cannot follow a
redirect, key URI or init-segment URI to any other destination, and the
signed playlist URL never appears in a process argument list.

Refused outright (typed PlaybackRefused, no retry here): encrypted playlists
(EXT-X-KEY other than NONE), any off-allowlist or non-HTTPS URI in a
playlist, 401/403/429, redirects, oversized responses.

Process ownership: a wall-clock deadline (checked between requests; each
request is bounded by its own timeout), an optional stop event, a media
duration cap, process-group terminate then kill then wait on every exit
path, and a bounded, redacted stderr tail. Nothing here is wired to a
command or session.
"""
from __future__ import annotations

import os
import select
import signal
import subprocess
import tempfile
import threading
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass

from . import twitch_playback as tp

SEGMENT_MAX_BYTES = 20_000_000
PLAYLIST_MAX_BYTES = 1_000_000


@dataclass
class PumpResult:
    reason: str          # ended | media_cap | deadline | stopped | ffmpeg_exit
    segments: int
    media_seconds: float
    bytes_written: int
    ffmpeg_rc: int
    ffmpeg_pid: int
    stderr_tail: str     # redacted, bounded


def _fetch(url, opener, host_ok, timeout, limit):
    if not tp._host_ok(url, host_ok):
        raise tp.PlaybackRefused("blocked", "URI outside the allowed playback hosts")
    code, body = opener(urllib.request.Request(
        url, headers={"User-Agent": "Mozilla/5.0"}), timeout)
    if code != 200:
        raise tp._status_refusal(code, "media")
    if len(body) > limit:
        raise tp.PlaybackRefused("error", "response too large")
    return body


def parse_media(text: str, base: str):
    """-> (segments[(seq,uri,dur)], map_uri|None, endlist, target_dur).
    Raises PlaybackRefused on encryption."""
    seq = 0
    segs, dur, map_uri, end, target = [], None, None, False, 6.0
    for ln in text.splitlines():
        ln = ln.strip()
        if ln.startswith("#EXT-X-MEDIA-SEQUENCE:"):
            try:
                seq = int(ln.split(":", 1)[1])
            except ValueError:
                raise tp.PlaybackRefused("error", "bad media sequence") from None
        elif ln.startswith("#EXT-X-TARGETDURATION:"):
            try:
                target = float(ln.split(":", 1)[1])
            except ValueError:
                pass
        elif ln.startswith("#EXT-X-KEY:"):
            if "METHOD=NONE" not in ln.upper():
                raise tp.PlaybackRefused("restricted", "encrypted stream")
        elif ln.startswith("#EXT-X-MAP:"):
            i = ln.find('URI="')
            if i < 0:
                raise tp.PlaybackRefused("error", "bad init segment")
            map_uri = urllib.parse.urljoin(base, ln[i + 5:ln.find('"', i + 5)])
        elif ln.startswith("#EXTINF:"):
            try:
                dur = float(ln.split(":", 1)[1].split(",")[0])
            except ValueError:
                dur = 0.0
        elif ln.startswith("#EXT-X-ENDLIST"):
            end = True
        elif ln and not ln.startswith("#"):
            segs.append((seq, urllib.parse.urljoin(base, ln), dur or 0.0))
            seq += 1
            dur = None
    return segs, map_uri, end, target


class _Deadline(Exception):
    pass


def _write(proc, data: bytes, t_end: float, clock, stop_event) -> None:
    """Write to ffmpeg's stdin without ever blocking past the deadline or a
    stop request (a wedged ffmpeg stops reading and a plain write() would
    block forever). Raises _Deadline / BrokenPipeError."""
    fd = proc.stdin.fileno()
    os.set_blocking(fd, False)
    view = memoryview(data)
    while view:
        if stop_event.is_set() or clock() >= t_end:
            raise _Deadline()
        _, w, _ = select.select([], [fd], [], 0.5)
        if not w:
            if proc.poll() is not None:
                raise BrokenPipeError()
            continue
        try:
            n = os.write(fd, view[:65536])
        except BlockingIOError:
            continue
        view = view[n:]


def _terminate(proc, grace):
    try:
        if proc.stdin:
            proc.stdin.close()
    except Exception:
        pass
    try:
        proc.wait(timeout=grace)
        return
    except subprocess.TimeoutExpired:
        pass
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(proc.pid, sig)
        except ProcessLookupError:
            break
        try:
            proc.wait(timeout=5)
            return
        except subprocess.TimeoutExpired:
            continue
    try:
        proc.wait(timeout=5)
    except Exception:
        pass


def capture(resolved: tp.Resolved, ffmpeg: str, out_path: str, *,
            max_media_seconds: float, deadline_sec: float,
            opener=None, host_ok=None, stop_event=None, timeout: float = 15.0,
            grace: float = 10.0, clock=time.monotonic,
            sleep=time.sleep) -> PumpResult:
    if not (0 < max_media_seconds <= 3600) or not (0 < deadline_sec <= 7200):
        raise ValueError("limits out of range")
    op = opener or tp.default_opener
    stop_event = stop_event or threading.Event()
    t_end = clock() + deadline_sec
    errf = tempfile.TemporaryFile()
    proc = subprocess.Popen(
        [ffmpeg, "-hide_banner", "-loglevel", "error", "-protocol_whitelist",
         "pipe", "-f", "mpegts", "-i", "pipe:0", "-t",
         str(int(max_media_seconds)), "-c", "copy", "-f", "mpegts", out_path],
        stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=errf,
        start_new_session=True)
    reason = "ended"
    segs_n, media, nbytes = 0, 0.0, 0
    err = None
    try:
        base = resolved.ffmpeg_input()
        next_seq, first, wrote_init = None, True, False
        while True:
            if stop_event.is_set():
                reason = "stopped"; break
            if clock() >= t_end:
                reason = "deadline"; break
            if proc.poll() is not None:
                reason = "ffmpeg_exit"; break
            try:
                text = _fetch(base, op, host_ok, timeout, PLAYLIST_MAX_BYTES
                              ).decode("utf-8", "replace")
            except tp.PlaybackRefused as e:
                if e.kind == "offline" and segs_n:
                    reason = "ended"; break
                raise
            segs, map_uri, end, target = parse_media(text, base)
            if map_uri and not wrote_init:
                blob = _fetch(map_uri, op, host_ok, timeout, SEGMENT_MAX_BYTES)
                _write(proc, blob, t_end, clock, stop_event)
                nbytes += len(blob); wrote_init = True
            if first and segs and not end:
                segs = segs[-3:]          # live: join near the live edge
            first = False
            for seq, uri, dur in segs:
                if next_seq is not None and seq < next_seq:
                    continue
                if stop_event.is_set() or clock() >= t_end or media >= max_media_seconds:
                    break
                try:
                    blob = _fetch(uri, op, host_ok, timeout, SEGMENT_MAX_BYTES)
                except tp.PlaybackRefused as e:
                    if e.kind == "offline":      # expired segment: skip
                        next_seq = seq + 1
                        continue
                    raise
                try:
                    _write(proc, blob, t_end, clock, stop_event)
                except _Deadline:
                    reason = "stopped" if stop_event.is_set() else "deadline"
                    break
                except (BrokenPipeError, OSError):
                    reason = "ffmpeg_exit"; break
                nbytes += len(blob); segs_n += 1; media += dur
                next_seq = seq + 1
            if reason in ("ffmpeg_exit", "deadline", "stopped"):
                break
            if media >= max_media_seconds:
                reason = "media_cap"; break
            if end:
                reason = "ended"; break
            sleep(min(max(target / 2.0, 0.2), 2.0))
    except tp.PlaybackRefused as e:
        err = e
    except Exception as exc:
        err = tp.PlaybackRefused("error", tp.redact(type(exc).__name__))
    finally:
        _terminate(proc, grace)
    try:
        errf.seek(0, os.SEEK_END)
        n = errf.tell(); errf.seek(max(0, n - 2000))
        tail = tp.redact(errf.read().decode("utf-8", "replace"))[-500:]
    finally:
        errf.close()
    if err is not None:
        raise err from None
    return PumpResult(reason, segs_n, media, nbytes, proc.returncode,
                      proc.pid, tail)
