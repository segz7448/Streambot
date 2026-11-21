"""Validated HLS capture: this process downloads, ffmpeg never touches the network.

Every playlist and segment request goes through the same opener and host
check as the resolver (HTTPS only, allowlisted hosts, redirects refused,
per-resource size limit with overflow detection, absolute deadline). Bytes
are piped to ffmpeg on stdin (``-f mpegts -i pipe:0``, ``pipe`` protocol
only), so ffmpeg cannot follow a redirect or URI anywhere and the signed
playlist URL never appears in a process argument list.

Supported: plain MPEG-TS media playlists. Refused with a typed error rather
than half-handled: encryption (any EXT-X-KEY whose METHOD is not exactly
NONE), EXT-X-MAP / fMP4, EXT-X-BYTERANGE, EXT-X-DISCONTINUITY, a missing
#EXTM3U header, non-finite or non-positive durations, any off-allowlist or
non-HTTPS URI, 401/403/429, redirects. (Refusing discontinuities means a
stream that stitches in an ad break ends the capture there.)

Process ownership: wall-clock deadline, stop callable, media-duration cap,
whole-process-group terminate/kill/wait on EVERY exit path even when the
leader already exited, stderr drained continuously into a bounded ring.
Nothing here is wired to a command or session.
"""
from __future__ import annotations

import math
import os
import re
import select
import signal
import subprocess
import threading
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass

from . import twitch_playback as tp

SEGMENT_MAX_BYTES = 20_000_000
PLAYLIST_MAX_BYTES = 1_000_000
STDERR_RING = 2000
_ATTR = re.compile(r'([A-Z0-9-]+)=("[^"]*"|[^,]*)')


@dataclass
class PumpResult:
    reason: str          # ended | media_cap | deadline | stopped | ffmpeg_exit
    segments: int
    media_seconds: float
    bytes_written: int
    ffmpeg_rc: int
    ffmpeg_pid: int
    stderr_tail: str     # redacted, bounded


def _fetch(url, opener, host_ok, timeout, limit, deadline):
    if not tp._host_ok(url, host_ok):
        raise tp.PlaybackRefused("blocked", "URI outside the allowed playback hosts")
    code, body = tp.call_opener(opener, urllib.request.Request(
        url, headers={"User-Agent": "Mozilla/5.0"}), timeout, limit, deadline)
    if code != 200:
        raise tp._status_refusal(code, "media")
    if len(body) > limit:
        raise tp.PlaybackRefused("error", "response too large")
    return body


def _attrs(line: str) -> dict:
    return {k: v.strip('"') for k, v in _ATTR.findall(line.split(":", 1)[1])}


def _pos_finite(text: str, what: str) -> float:
    try:
        v = float(text)
    except ValueError:
        raise tp.PlaybackRefused("error", f"bad {what}") from None
    if not math.isfinite(v) or v <= 0:
        raise tp.PlaybackRefused("error", f"bad {what}")
    return v


def parse_media(text: str, base: str):
    """-> (segments[(seq,uri,dur)], endlist, target_dur)."""
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if not lines or lines[0] != "#EXTM3U":
        raise tp.PlaybackRefused("error", "not an HLS playlist")
    seq, segs, dur, end, target = 0, [], None, False, 6.0
    for ln in lines[1:]:
        if ln.startswith("#EXT-X-MEDIA-SEQUENCE:"):
            try:
                seq = int(ln.split(":", 1)[1])
            except ValueError:
                raise tp.PlaybackRefused("error", "bad media sequence") from None
            if seq < 0:
                raise tp.PlaybackRefused("error", "bad media sequence")
        elif ln.startswith("#EXT-X-TARGETDURATION:"):
            target = _pos_finite(ln.split(":", 1)[1], "target duration")
        elif ln.startswith("#EXT-X-KEY:"):
            if _attrs(ln).get("METHOD") != "NONE":
                raise tp.PlaybackRefused("restricted", "encrypted stream")
        elif ln.startswith(("#EXT-X-MAP:", "#EXT-X-BYTERANGE:",
                            "#EXT-X-DISCONTINUITY")):
            raise tp.PlaybackRefused("error", "unsupported playlist feature")
        elif ln.startswith("#EXTINF:"):
            dur = _pos_finite(ln.split(":", 1)[1].split(",")[0], "segment duration")
        elif ln.startswith("#EXT-X-ENDLIST"):
            end = True
        elif not ln.startswith("#"):
            if dur is None:
                raise tp.PlaybackRefused("error", "segment without duration")
            segs.append((seq, urllib.parse.urljoin(base, ln), dur))
            seq += 1
            dur = None
    return segs, end, target


class _Deadline(Exception):
    pass


def _write(proc, data: bytes, t_end, clock, should_stop) -> None:
    fd = proc.stdin.fileno()
    os.set_blocking(fd, False)
    view = memoryview(data)
    while view:
        if should_stop() or clock() >= t_end:
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


def _group_alive(pgid) -> bool:
    try:
        os.killpg(pgid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _terminate(proc, grace):
    """Close stdin, give the leader a grace period, then take down the WHOLE
    process group whether or not the leader already exited."""
    try:
        if proc.stdin:
            proc.stdin.close()
    except Exception:
        pass
    try:
        proc.wait(timeout=grace)
    except subprocess.TimeoutExpired:
        pass
    pgid = proc.pid
    for sig, wait in ((signal.SIGTERM, 2.0), (signal.SIGKILL, 5.0)):
        if not _group_alive(pgid):
            break
        try:
            os.killpg(pgid, sig)
        except ProcessLookupError:
            break
        t0 = time.monotonic()
        while _group_alive(pgid) and time.monotonic() - t0 < wait:
            time.sleep(0.05)
    try:
        proc.wait(timeout=5)
    except Exception:
        pass


def capture(resolved: tp.Resolved, ffmpeg: str, out_path: str, *,
            max_media_seconds: float, deadline_sec: float,
            opener=None, host_ok=None, should_stop=None, timeout: float = 15.0,
            grace: float = 10.0, clock=time.monotonic, sleep=time.sleep,
            output_args=None) -> PumpResult:
    """Capture to out_path (single MPEG-TS file), or pass ``output_args``
    (list placed after the input, ending in the output target) for e.g. a
    segmenter. ``should_stop()`` is polled before every request and write."""
    if not (0 < max_media_seconds <= 3600) or not (0 < deadline_sec <= 7200):
        raise ValueError("limits out of range")
    op = opener or tp.default_opener
    should_stop = should_stop or (lambda: False)
    t_end = clock() + deadline_sec
    dl = time.monotonic() + deadline_sec
    out = output_args or ["-c", "copy", "-f", "mpegts", out_path]
    proc = subprocess.Popen(
        [ffmpeg, "-hide_banner", "-loglevel", "error", "-protocol_whitelist",
         "pipe", "-f", "mpegts", "-i", "pipe:0", "-t",
         str(int(max_media_seconds))] + out,
        stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE, start_new_session=True)
    ring = bytearray()

    def drain():
        try:
            while True:
                b = proc.stderr.read(4096)
                if not b:
                    break
                ring.extend(b)
                if len(ring) > STDERR_RING:
                    del ring[:len(ring) - STDERR_RING]
        except Exception:
            pass
    th = threading.Thread(target=drain, daemon=True)
    th.start()
    reason, segs_n, media, nbytes, err = "ended", 0, 0.0, 0, None
    try:
        base = resolved.ffmpeg_input()
        next_seq, first = None, True
        while True:
            if should_stop():
                reason = "stopped"; break
            if clock() >= t_end:
                reason = "deadline"; break
            if proc.poll() is not None:
                reason = "ffmpeg_exit"; break
            try:
                text = _fetch(base, op, host_ok, timeout, PLAYLIST_MAX_BYTES,
                              dl).decode("utf-8", "replace")
            except tp.PlaybackRefused as e:
                if e.kind == "offline" and segs_n:
                    reason = "ended"; break
                raise
            segs, end, target = parse_media(text, base)
            if first and segs and not end:
                segs = segs[-3:]          # live: join near the live edge
            first = False
            for seq, uri, dur in segs:
                if next_seq is not None and seq < next_seq:
                    continue
                if should_stop() or clock() >= t_end or media >= max_media_seconds:
                    break
                try:
                    blob = _fetch(uri, op, host_ok, timeout, SEGMENT_MAX_BYTES, dl)
                except tp.PlaybackRefused as e:
                    if e.kind == "offline":      # expired segment: skip
                        next_seq = seq + 1
                        continue
                    raise
                try:
                    _write(proc, blob, t_end, clock, should_stop)
                except _Deadline:
                    reason = "stopped" if should_stop() else "deadline"
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
        th.join(timeout=5)
        try:
            proc.stderr.close()
        except Exception:
            pass
    tail = tp.redact(bytes(ring).decode("utf-8", "replace"))[-500:]
    if err is not None:
        raise err from None
    return PumpResult(reason, segs_n, media, nbytes, proc.returncode,
                      proc.pid, tail)
