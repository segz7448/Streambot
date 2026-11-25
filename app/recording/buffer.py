"""Rolling segmented media buffer.

FFmpeg writes fixed-duration segments; the buffer keeps enough footage
before and after detected events and enforces a storage cap with safe
rotation. Segment metadata lives in the database so a restarted process can
reconcile on-disk segments instead of duplicating recordings.
"""
from __future__ import annotations

import itertools
import json
import math
import os
import signal
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path

from ..core.database import connect
from ..core.logging import get_logger

log = get_logger("recording.buffer")


class TimelineError(Exception):
    """Persisted timeline could not be written durably. Explicit and
    fail-closed: a caller must never expose a timeline it could not
    persist as durable."""


_PROBE_TIMEOUT = 30.0           # per-probe wall cap (seconds)
_PROBE_PASS_BUDGET = 30.0        # AGGREGATE probe budget per pass:
                                 # every launch gets the shared remaining
                                 # time, so one pass never exceeds this
_PROBE_OUTPUT_MAX = 4096         # duration output is a few bytes
_PROBE_CACHE_MAX = 1024          # bounded per-process probe cache
_TIMELINE_DURATIONS_MAX = 65536  # bounded persisted sidecar (~4d of 5s)
_MAX_TS = 4102444800.0           # 2100-01-01: anchor sanity range
_MAX_DURATION = 86400.0          # one day: duration sanity range

# In-process diagnostic only. Production ownership is the durable session
# intent/bind registry passed below, which retains the pid and pgid history.
# Remove diagnostics only after signalling the group and reaping the leader.
_PROBE_REGISTRY: dict = {}
_PROBE_REGISTRY_LOCK = threading.Lock()


def _kill_group(proc) -> None:
    # pid is the group id even after the leader exits. Never rediscover it
    # with getpgid: the leader may already have been reaped.
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def ffprobe_probe(ffprobe: str, pre_spawn=None, on_spawn=None):
    """Stream at most 4097 bytes, with one deadline through EOF and reap.

    Production uses the session's durable intent/bind callbacks. Both or
    neither must be supplied. Standalone callers own the synchronous call.
    Every exit kills the owned group, including an exited leader's peers.
    """
    if (pre_spawn is None) != (on_spawn is None):
        raise ValueError("probe lifecycle callbacks must be paired")

    def probe(path, timeout: float = _PROBE_TIMEOUT) -> float:
        import select
        deadline = time.monotonic() + min(_PROBE_TIMEOUT, timeout)
        if not math.isfinite(timeout) or timeout <= 0:
            raise RuntimeError("ffprobe deadline")
        args = [ffprobe, "-v", "error", "-protocol_whitelist", "file",
                "-show_entries", "format=duration", "-of", "csv=p=0",
                str(path)]
        token = None
        if pre_spawn is not None:
            try:
                token = pre_spawn()
            except Exception:
                raise RuntimeError("ffprobe intent failed") from None
            if token is None:
                raise RuntimeError("ffprobe intent failed")
        if time.monotonic() >= deadline:
            raise RuntimeError("ffprobe deadline")
        producer = subprocess.Popen(
            args, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            start_new_session=True)
        entry = {"pid": producer.pid, "started_at": time.time(),
                 "timeout": timeout}
        with _PROBE_REGISTRY_LOCK:
            _PROBE_REGISTRY[producer.pid] = entry
        try:
            if on_spawn is not None:
                try:
                    bound = on_spawn(producer.pid, producer.pid, token)
                except Exception:
                    bound = False
                if not bound:
                    raise RuntimeError("ffprobe identity failed")
            data = bytearray()
            fd = producer.stdout.fileno()
            while True:
                left = deadline - time.monotonic()
                if left <= 0:
                    raise RuntimeError("ffprobe deadline")
                ready, _, _ = select.select([fd], [], [], min(left, 0.1))
                if not ready:
                    continue
                chunk = os.read(fd, _PROBE_OUTPUT_MAX + 1 - len(data))
                if not chunk:
                    break
                data.extend(chunk)
                if len(data) > _PROBE_OUTPUT_MAX:
                    raise RuntimeError("ffprobe output limit")
            left = deadline - time.monotonic()
            if left <= 0:
                raise RuntimeError("ffprobe deadline")
            try:
                rc = producer.wait(timeout=left)
            except subprocess.TimeoutExpired:
                raise RuntimeError("ffprobe deadline") from None
            if rc != 0:
                raise RuntimeError("ffprobe failed")
            try:
                return float(data.decode().strip())
            except (ValueError, UnicodeError):
                raise RuntimeError("ffprobe invalid duration") from None
        finally:
            # Always signal the original group, not only a live leader.
            # Keep lifecycle evidence on any uncertain cleanup failure.
            _kill_group(producer)
            producer.wait()
            producer.stdout.close()
            with _PROBE_REGISTRY_LOCK:
                _PROBE_REGISTRY.pop(producer.pid, None)
    return probe


@dataclass
class Segment:
    session_id: int
    sequence: int
    path: Path
    duration_seconds: float
    created_at: float


class RollingBuffer:
    def __init__(self, root: str, session_id: int, segment_seconds: int = 5,
                 pre_event_seconds: int = 30, post_event_seconds: int = 30,
                 max_storage_mb: int = 2048, probe=None):
        self.root = Path(root)
        self.session_id = session_id
        self.segment_seconds = segment_seconds
        self.pre_event_segments = max(1, pre_event_seconds // segment_seconds)
        self.post_event_segments = max(1, post_event_seconds // segment_seconds)
        self.max_storage_bytes = max_storage_mb * 1024 * 1024
        # probe(path) -> actual container duration; keyframe-aligned cuts
        # (-c copy -f segment) make real durations differ from the nominal
        # segment_time (measured: 6.02/4.06/6.06/4.05 for a 5s target on a
        # 2s GOP; session 8 on-device: 6/2/4/4). None keeps nominal timing.
        self.probe = probe
        self._probe_cache: dict = {}
        # every sequence at or below this was final at drain attestation
        self._drained_through = -1
        # guards timeline adoption/persist and the probe cache against
        # concurrent callers (reconcile thread vs detection thread)
        self._lock = threading.Lock()
        self._tl = None            # in-process adopted timeline, once
        self._tl_tmp = itertools.count()
        self._db_path = None       # bound by reconcile()/bind_db()
        self.dir.mkdir(parents=True, exist_ok=True)

    def bind_db(self, db_path: str) -> None:
        """Register the session database: its recording_segments rows are
        the authoritative per-sequence identity (legacy rows keep their
        recorded duration/created_at verbatim; the timeline never
        reinterprets them)."""
        with self._lock:
            self._db_path = str(db_path)

    def mark_drained(self) -> None:
        """Attest that no writer will append to any current segment again
        (all writers stopped). Closes the open tail so its duration is
        probed and persisted exactly once."""
        seqs = [int(p.stem.split("-")[1]) for p in self.dir.glob("seg-*.ts")]
        with self._lock:
            if seqs:
                self._drained_through = max(max(seqs),
                                            self._drained_through)

    def _closed_sequences(self, by_seq: dict) -> set:
        """A segment is CLOSED - its duration will never change - once a
        higher sequence exists on disk (the muxer appends only to the
        tail) or it was present at drain attestation. Only closed
        segments are ever probed/persisted; the open tail keeps nominal
        timing so a growing file never rewrites history."""
        if not by_seq:
            return set()
        mx = max(by_seq)
        return {q for q in by_seq if q < mx or q <= self._drained_through}

    def _duration(self, path: Path, timeout: float) -> float | None:
        """Actual probed duration of a CLOSED segment, cached per
        (size, mtime); None when the probe did not resolve this pass -
        the caller WITHHOLDS the segment and its dependents rather than
        publish a nominal value a later pass would rewrite. `timeout` is
        the shared per-pass remaining budget, so aggregate probe work in
        one pass is capped (the capture/Telegram loops stay responsive).
        Never called for the open tail."""
        if self.probe is None:
            return float(self.segment_seconds)  # probing disabled: stable
        st = path.stat()
        ent = self._probe_cache.get(path)
        if ent is not None and ent[0] == (st.st_size, st.st_mtime):
            return ent[1]
        try:
            try:
                dur = float(self.probe(path, timeout=timeout))
            except TypeError:
                dur = float(self.probe(path))  # injected one-arg callables
            if not 0.0 < dur < 3600.0:
                raise ValueError("implausible duration")
        except Exception:
            log.warning("duration probe failed for %s; withholding "
                        "segment this pass", path)
            return None
        if len(self._probe_cache) >= _PROBE_CACHE_MAX:
            self._probe_cache.pop(next(iter(self._probe_cache)))
        self._probe_cache[path] = ((st.st_size, st.st_mtime), dur)
        return dur

    def _timeline_path(self) -> Path:
        return self.dir / ".timeline.json"

    def _load_timeline(self):
        """(state, value) with three DISTINCT states:
        ('ok', (anchor_seq, anchor_t, durations)) - validated sidecar.
        ('absent', None) - no sidecar: one-time adoption is permitted
          (subject to prior DB/event identity, see _adopt_locked).
        ('invalid', None) - sidecar exists but is unreadable or fails
          schema/type/finite/range validation: NEVER re-anchor over it -
          that would rewrite history on top of unreadable durable state -
          and never invent nominal replacement times for it.
        """
        try:
            raw = self._timeline_path().read_text()
        except FileNotFoundError:
            return "absent", None
        except OSError:
            log.warning("session %s timeline sidecar unreadable; "
                        "not re-anchoring", self.session_id)
            return "invalid", None
        try:
            d = json.loads(raw)
            if not isinstance(d, dict):
                raise ValueError("not an object")
            aseq, at = d["anchor_seq"], d["anchor_t"]
            if (isinstance(aseq, bool) or not isinstance(aseq, int)
                    or aseq < 0):
                raise ValueError("anchor_seq")
            if (isinstance(at, bool) or not isinstance(at, (int, float))
                    or not math.isfinite(at) or not 0 <= at <= _MAX_TS):
                raise ValueError("anchor_t")
            durs = d.get("durations", {})
            if not isinstance(durs, dict):
                raise ValueError("durations")
            if len(durs) > _TIMELINE_DURATIONS_MAX:
                raise ValueError("durations bound")
            durations = {}
            for k, v in durs.items():
                if not isinstance(k, str) or not k.isdigit():
                    raise ValueError("duration key")
                q = int(k)
                if (isinstance(v, bool)
                        or not isinstance(v, (int, float))
                        or not math.isfinite(v)
                        or not 0 < v <= _MAX_DURATION):
                    raise ValueError("duration value")
                durations[q] = float(v)
            return "ok", (aseq, float(at), durations)
        except (ValueError, KeyError, TypeError, AttributeError):
            log.warning("session %s timeline sidecar invalid; "
                        "not re-anchoring", self.session_id)
            return "invalid", None

    def _save_timeline(self, anchor_seq: int, anchor_t: float,
                       durations: dict) -> None:
        """Durable write, fsync-ordered: unique tmp file -> fsync(file)
        -> replace -> fsync(directory). Any failure raises TimelineError
        and the tmp file is removed; a partially written sidecar is never
        left behind."""
        tmp = self.dir / (".timeline.json.%d.%d.tmp"
                          % (os.getpid(), next(self._tl_tmp)))
        payload = json.dumps(
            {"anchor_seq": anchor_seq, "anchor_t": anchor_t,
             "durations": {str(k): v for k, v in durations.items()}},
            sort_keys=True)
        try:
            fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
            try:
                os.write(fd, payload.encode())
                os.fsync(fd)
            finally:
                os.close(fd)
            os.replace(tmp, self._timeline_path())
            dfd = os.open(self.dir, os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
        except OSError as exc:
            try:
                tmp.unlink()
            except OSError:
                pass
            raise TimelineError(
                "timeline persist failed for session %s"
                % self.session_id) from exc

    def _db_identity(self):
        """({seq: (duration_or_None, created_at)}, events_exist) from the
        bound database: recording_segments rows are validated settled
        identity. A KNOWN unreadable database is treated as 'events may
        exist' so adoption is refused rather than guessed over."""
        if self._db_path is None:
            return {}, False
        try:
            conn = connect(self._db_path)
            try:
                rows = {}
                for q, dur, ct in conn.execute(
                        "SELECT sequence, duration_seconds, created_at"
                        " FROM recording_segments WHERE session_id=?",
                        (self.session_id,)):
                    if (not isinstance(q, int) or q < 0
                            or not isinstance(ct, (int, float))
                            or not math.isfinite(ct)
                            or not 0 <= ct <= _MAX_TS):
                        continue
                    d = None
                    if (isinstance(dur, (int, float))
                            and not isinstance(dur, bool)
                            and math.isfinite(dur)
                            and 0 < dur <= _MAX_DURATION):
                        d = float(dur)
                    rows[q] = (d, float(ct))
                ev = conn.execute(
                    "SELECT 1 FROM candidate_events WHERE session_id=?"
                    " LIMIT 1", (self.session_id,)).fetchone() is not None
                return rows, ev
            finally:
                conn.close()
        except Exception:
            log.warning("session %s DB identity unreadable; refusing "
                        "timeline adoption", self.session_id)
            return {}, True  # unknown prior identity: fail closed

    def _adopt_locked(self, by_seq: dict, closed: set, db_rows: dict,
                      db_events: bool):
        """One-time anchor adoption, only onto SETTLED identity:
        - validated DB rows exist: anchor = earliest row's recorded
          created_at (identity already durable; sidecar write failure is
          tolerated, the DB is the store of truth);
        - no rows and no recorded events: anchor = earliest CLOSED
          segment's final mtime, and it MUST persist - an unpersisted new
          timeline is never exposed (TimelineError propagates);
        - recorded events without segment rows: a fresh mtime anchor
          could reinterpret those events - refuse (None).
        Returns (anchor_seq, anchor_t, durations) or None."""
        if db_rows:
            a = min(db_rows)
            tl = (a, db_rows[a][1], {})
            try:
                self._save_timeline(*tl)
            except TimelineError:
                log.warning("session %s sidecar persist failed; DB rows "
                            "remain the durable identity",
                            self.session_id)
            self._tl = tl
            return tl
        if db_events:
            log.error("session %s has recorded events but no segment "
                      "rows; refusing timeline adoption", self.session_id)
            return None
        if not closed:
            return None  # only an open tail: provisional, nothing durable
        a = min(closed)
        tl = (a, by_seq[a].stat().st_mtime, {})
        self._save_timeline(*tl)   # raises; caller fails closed
        self._tl = tl
        return tl

    @property
    def dir(self) -> Path:
        return self.root / f"session-{self.session_id}"

    def next_seq(self) -> int:
        """Next segment sequence number. A restarted ffmpeg must not
        restart at seg-000000 and overwrite or rewind content time:
        numbering continues after the newest on-disk segment."""
        seqs = [int(p.stem.split("-")[1])
                for p in self.dir.glob("seg-*.ts")]
        return max(seqs) + 1 if seqs else 0

    def segment_args(self, input_url: str,
                     start_number: int = 0) -> list[str]:
        """FFmpeg args producing numbered segments for this session.
        Protocol access is whitelisted per feed kind: a local recording
        may open files only; a registered http(s) feed may speak only
        the protocols that carry it (redirects to file:// or other
        schemes are refused by ffmpeg). start_number continues segment
        numbering after a reconnect so sequence-derived content time
        stays monotonic."""
        pattern = str(self.dir / "seg-%06d.ts")
        whitelist = ("http,https,tcp,tls"
                     if input_url.startswith(("http://", "https://"))
                     else "file")
        args = ["-hide_banner", "-loglevel", "warning",
                "-protocol_whitelist", whitelist,
                "-i", input_url, "-c", "copy",
                "-f", "segment", "-segment_time",
                str(self.segment_seconds),
                "-reset_timestamps", "1"]
        if start_number:
            # the segment muxer's option is -segment_start_number;
            # plain -start_number is image2-only and silently ignored
            args += ["-segment_start_number", str(start_number)]
        return args + [pattern]

    def segmenter_output_args(self, start_number: int = 0) -> list[str]:
        """Output-side args only (everything after the input), for a caller
        that feeds ffmpeg its own input (hls_pump pipes MPEG-TS to stdin)."""
        pattern = str(self.dir / "seg-%06d.ts")
        args = ["-c", "copy", "-f", "segment", "-segment_time",
                str(self.segment_seconds), "-reset_timestamps", "1"]
        if start_number:
            args += ["-segment_start_number", str(start_number)]
        return args + [pattern]

    def segments(self) -> list[Segment]:
        """Segment list on a STABLE content-time axis. The timeline
        (anchor + per-segment actual durations) is adopted once and
        persisted in .timeline.json; it never rebases across segment
        growth, rotation, or process restart, so an event time recorded
        against it keeps pointing at the same content. Segments missing
        from disk AND history (rotated before ever being closed) keep the
        nominal length: their true duration is unknowable and rebasing
        would reinterpret recorded events. The open tail (still written)
        is never probed and carries nominal duration; it closes at the
        next segment or at drain attestation."""
        paths = sorted(self.dir.glob("seg-*.ts"))
        if not paths:
            return []
        by_seq = {int(p.stem.split("-")[1]): p for p in paths}
        with self._lock:
            return self._segments_locked(by_seq)

    def _segments_locked(self, by_seq: dict) -> list[Segment]:
        closed = self._closed_sequences(by_seq)
        db_rows, db_events = self._db_identity()
        if self._tl is not None:
            tl = self._tl
        else:
            state, loaded = self._load_timeline()
            if state == "ok":
                tl = self._tl = loaded
            elif state == "absent":
                try:
                    tl = self._adopt_locked(by_seq, closed, db_rows,
                                            db_events)
                except TimelineError:
                    # a NEW timeline that could not be persisted is never
                    # exposed as usable durable timing
                    log.error("session %s: %s", self.session_id,
                              "timeline adoption failed; no usable "
                              "segments this pass")
                    return []
            else:
                # invalid/corrupt persisted state: no re-anchor, no
                # nominal replacement times; only DB-settled identity
                # is usable
                log.error("session %s: persisted timeline invalid; only "
                          "DB-settled segments usable", self.session_id)
                tl = None
        if tl is None:
            if db_rows:
                return [Segment(self.session_id, q, by_seq[q],
                                d if d is not None
                                else float(self.segment_seconds), c)
                        for q, (d, c) in sorted(db_rows.items())
                        if q in by_seq]
            if db_events or self._tl is None and self._timeline_path().exists():
                return []  # refused adoption / invalid persisted state
            # anchorless only-open-tail dir (nothing durable exists yet):
            # provisional mtime values, never persisted, never reused
            return [Segment(self.session_id, q, p,
                            float(self.segment_seconds),
                            p.stat().st_mtime)
                    for q, p in sorted(by_seq.items())]
        anchor_seq, anchor_t, persisted = tl
        durations = dict(persisted)
        probed_new: dict[int, float] = {}
        unresolved: list[int] = []
        deadline = time.monotonic() + _PROBE_PASS_BUDGET
        for q in sorted(closed):
            if q in durations or q in db_rows:
                continue  # settled identity; never reinterpreted
            remaining = deadline - time.monotonic()
            if remaining <= 0.0:
                unresolved.append(q)   # aggregate probe budget exhausted
                continue
            dur = self._duration(by_seq[q], timeout=remaining)
            if dur is None:
                unresolved.append(q)
            else:
                probed_new[q] = dur
        if probed_new:
            merged = dict(durations)
            merged.update(probed_new)
            if len(merged) <= _TIMELINE_DURATIONS_MAX:
                try:
                    self._save_timeline(anchor_seq, anchor_t, merged)
                    self._tl = (anchor_seq, anchor_t, dict(merged))
                    durations = merged
                except TimelineError:
                    # WITHHOLDING INVARIANT: an uncommitted NEW duration
                    # is never usable, even though a closed file reprobes
                    # deterministically - a FRESH instance (or this one
                    # after restart) has only the committed state, and no
                    # instance may expose identity another cannot settle.
                    # The sequences stay withheld (with dependents) until
                    # a durable commit; probing is retried each pass.
                    log.warning("session %s duration persist failed; "
                                "withholding %d uncommitted segment(s)",
                                self.session_id, len(probed_new))
                    unresolved.extend(sorted(probed_new))
            else:
                log.warning("session %s durations bound reached; "
                            "withholding new probes", self.session_id)
                unresolved.extend(sorted(probed_new))
        withheld: set = set()
        if unresolved:
            # an unresolved CLOSED duration makes its own AND every
            # higher sequence's content time unknowable for now: withhold
            # them all until a durable commit, rather than expose
            # transient nominal timing. DB-settled rows stay usable.
            first = min(unresolved)
            withheld = {q for q in by_seq if q >= first and q not in db_rows}
            log.warning("session %s: %d closed segment(s) unresolved; "
                        "withholding %d until durably committed",
                        self.session_id, len(unresolved), len(withheld))
        nominal = float(self.segment_seconds)
        axis = {q: d for q, (d, c) in db_rows.items() if d is not None}
        axis.update(durations)  # probed values for rows without one

        def created(seq: int) -> float:
            if seq in db_rows:
                return db_rows[seq][1]     # settled identity, verbatim
            if seq <= anchor_seq:
                return anchor_t - (anchor_seq - seq) * nominal
            known = {i: d for i, d in axis.items()
                     if anchor_seq < i <= seq}
            return anchor_t + sum(known.values()) + (
                seq - anchor_seq - len(known)) * nominal

        return [Segment(self.session_id, seq, path,
                        db_rows[seq][0] if seq in db_rows
                        and db_rows[seq][0] is not None
                        else durations.get(seq, nominal),
                        created(seq))
                for seq, path in sorted(by_seq.items())
                if seq not in withheld]

    def event_window(self, event_time: float) -> list[Path]:
        """Segments whose content interval [created-duration, created]
        OVERLAPS the pre/post-event retention window. Point-in-window
        membership on the end time alone drops a segment that carries the
        window's edge content (e.g. a 6s segment whose end falls just
        outside)."""
        segs = self.segments()
        if not segs:
            return []
        start = event_time - self.pre_event_segments * self.segment_seconds
        end = event_time + self.post_event_segments * self.segment_seconds
        return [s.path for s in segs
                if s.created_at > start
                and s.created_at - s.duration_seconds < end]

    def _pending_mark_floor(self) -> float | None:
        """Earliest content time still needed by a pending manual mark
        (its moment minus the pre-event window); None when none pending.
        Pre-roll for a waiting mark must not be rotated away, but only until
        its wait deadline: a hung tick loop can never pin an unbounded
        growing capture past that bounded wait."""
        if self._db_path is None:
            return None
        try:
            conn = connect(self._db_path)
            try:
                at = conn.execute(
                    "SELECT MIN(detected_at) FROM candidate_events"
                    " WHERE session_id=? AND kind='manual_mark'"
                    " AND status IN ('PENDING','QUEUED')"
                    " AND (json_extract(metadata_json,'$.wait_deadline_at')"
                    " IS NULL OR json_extract(metadata_json,"
                    "'$.wait_deadline_at') > ?)",
                    (self.session_id, time.time())).fetchone()[0]
            finally:
                conn.close()
        except Exception:
            return None
        if at is None:
            return None
        return float(at) - self.pre_event_segments * self.segment_seconds

    def enforce_storage_cap(self, keep_after: float | None = None,
                            protect_newest: int = 2) -> int:
        """Delete oldest segments until under the cap. The newest segments
        (one may be actively written by ffmpeg) and segments inside the
        retention window of an event (keep_after) are preserved."""
        floor = self._pending_mark_floor()
        if floor is not None:
            keep_after = floor if keep_after is None else min(keep_after,
                                                              floor)
        segs = self.segments()
        total = sum(s.path.stat().st_size for s in segs)
        removed = 0
        protected = {s.sequence for s in segs[-protect_newest:]} if segs else set()
        for seg in segs:
            if total <= self.max_storage_bytes:
                break
            if seg.sequence in protected:
                continue
            if keep_after is not None and seg.created_at >= keep_after:
                continue
            try:
                size = seg.path.stat().st_size
                seg.path.unlink()
                total -= size
                removed += 1
            except FileNotFoundError:
                pass
        if removed:
            log.info("buffer rotation removed %d segments (session %s)",
                     removed, self.session_id)
        return removed

    def reconcile(self, db_path: str) -> int:
        """Record on-disk segments missing from the database (post-restart)."""
        self.bind_db(db_path)
        conn = connect(db_path)
        try:
            known = {r["sequence"] for r in conn.execute(
                "SELECT sequence FROM recording_segments WHERE session_id = ?",
                (self.session_id,))}
            added = 0
            with conn:
                closed = self._closed_sequences(
                    {int(p.stem.split("-")[1]): p
                     for p in self.dir.glob("seg-*.ts")})
                for seg in self.segments():
                    if seg.sequence in known:
                        continue
                    if seg.sequence not in closed:
                        continue  # open tail: no row until values are final
                    conn.execute(
                        "INSERT INTO recording_segments"
                        " (session_id, file_path, sequence, duration_seconds,"
                        "  created_at) VALUES (?, ?, ?, ?, ?)",
                        (self.session_id, str(seg.path), seg.sequence,
                         seg.duration_seconds, seg.created_at))
                    added += 1
            return added
        finally:
            conn.close()
