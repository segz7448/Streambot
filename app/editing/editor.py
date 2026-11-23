"""Phase 011: edit a source clip into platform-ready outputs.

Pure filter-graph construction separated from execution. Vertical/square
outputs center-crop then scale; subtitles burn via libass when a subtitle
file is supplied; audio is loudness-normalized. All filters use codecs and
filters verified present in the on-device ffmpeg 9.0.2 build.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

from ..bot.restartproof import claim_owner_dead, read_start_time
from ..subtitles.fonts import ensure_fontconfig_env
from .presets import OutputPreset


def crop_filter(preset: OutputPreset) -> str:
    """Center-crop to the preset aspect ratio, then scale to target size."""
    target_ar = preset.width / preset.height
    # crop keeps full height for portrait/square targets, full width otherwise
    return (
        f"crop=w='if(gt(a,{target_ar:.6f}),ih*{target_ar:.6f},iw)'"
        f":h='if(gt(a,{target_ar:.6f}),ih,iw/{target_ar:.6f})',"
        f"scale={preset.width}:{preset.height}"
    )


def _escape_drawtext(text: str) -> str:
    """Owner-supplied watermark text for ffmpeg drawtext. Collapsed to a
    single bounded line with every filter-graph metacharacter escaped;
    the value travels as one argv element (never a shell), so this only
    needs to keep the filter graph itself intact."""
    t = " ".join(text.split())[:100]
    for ch, rep in (("\\", "\\\\"), (":", "\\:"), ("'", "\\'"),
                    ("%", "\\%"), (",", "\\,"), (";", "\\;"),
                    ("[", "\\["), ("]", "\\]")):
        t = t.replace(ch, rep)
    return t


def full_frame_filter(preset: OutputPreset) -> str:
    """Content-preserving representation: NO source pixel is cropped.
    The full frame is scaled to the preset width and centered on the
    preset canvas over a blurred fill derived from the same frame
    (deterministic, no vision, no per-source layout assumption). For a
    16:9 source into the vertical preset this keeps overlays that a
    center crop discards (e.g. a corner facecam), at the cost of a
    smaller active picture; the canvas is still an upscale of the
    source, never new detail."""
    return (
        f"split=2[src_bg][src_fg];"
        f"[src_bg]scale={preset.width}:{preset.height}:"
        f"force_original_aspect_ratio=increase:force_divisible_by=2,"
        f"crop={preset.width}:{preset.height},boxblur=20:2[bg];"
        f"[src_fg]scale={preset.width}:-2[fg];"
        f"[bg][fg]overlay=(W-w)/2:(H-h)/2"
    )


def build_filter_graph(preset: OutputPreset,
                       subtitle_path: str | None = None,
                       focus_x: float = 0.5,
                       watermark: str | None = None,
                       framing: str = "crop") -> str:
    """Video filter chain. framing="crop" center-crops to the preset
    aspect (focus_x shifts the window: 0=left, 1=right); framing="full"
    preserves every source pixel via full_frame_filter. Subtitles are
    burned last so they render at output resolution. An owner watermark
    is drawn bottom-right via drawtext."""
    if framing == "full":
        chain = full_frame_filter(preset)
    else:
        target_ar = preset.width / preset.height
        x_expr = (f"'if(gt(a,{target_ar:.6f}),(iw-ih*{target_ar:.6f})*{focus_x:.3f},0)'")
        chain = (
            f"crop=w='if(gt(a,{target_ar:.6f}),ih*{target_ar:.6f},iw)'"
            f":h='if(gt(a,{target_ar:.6f}),ih,iw/{target_ar:.6f})':x={x_expr}:y=0,"
            f"scale={preset.width}:{preset.height}"
        )
    if watermark:
        chain += (",drawtext=text='" + _escape_drawtext(watermark) +
                  "':x=w-text_w-24:y=h-text_h-24:fontsize=36"
                  ":fontcolor=white:borderw=2:bordercolor=black")
    if subtitle_path:
        escaped = subtitle_path.replace(":", "\\:").replace("'", "\\'")
        chain += f",subtitles='{escaped}'"
    return chain


def edit_args(ffmpeg: str, src: str, out: str, preset: OutputPreset,
              subtitle_path: str | None = None, focus_x: float = 0.5,
              loudnorm: bool = True, watermark: str | None = None,
              framing: str = "crop") -> list[str]:
    args = [ffmpeg, "-y", "-i", src,
            "-vf", build_filter_graph(preset, subtitle_path, focus_x,
                                      watermark, framing),
            "-c:v", "libx264", "-preset", "veryfast",
            "-crf", str(preset.video_crf), "-pix_fmt", "yuv420p",
            "-c:a", "aac", "-b:a", preset.audio_bitrate,
            "-t", str(preset.max_duration_seconds)]
    if loudnorm:
        args += ["-af", "loudnorm=I=-16:TP=-1.5:LRA=11"]
    args += ["-movflags", "+faststart", out]
    return args


def _close_pipes(proc) -> None:
    """Close any pipes still open on the child so no fd reaches a
    destructor unclosed (ResourceWarning)."""
    for name in ("stdin", "stdout", "stderr"):
        stream = getattr(proc, name, None)
        try:
            if stream is not None:
                stream.close()
        except Exception:
            pass


def _group_alive(pgid: int) -> bool:
    """True unless the group is verifiably EMPTY. Any uncertainty
    (permission, transient errors) is treated as alive: group cleanup
    must never declare success on a guess."""
    import os
    try:
        os.killpg(pgid, 0)
        return True
    except ProcessLookupError:
        return False
    except Exception:
        return True


def _kill_group(proc) -> None:
    """Best-effort teardown of the spawned child's own process group (the
    child leads it: start_new_session=True). A dead leader is NOT a dead
    group: after the leader exits we verify the group is empty and
    escalate to SIGKILL while any member survives. This is group cleanup
    only; it does not establish arbitrary-descendant containment, and
    callers verify outcomes instead of trusting this best-effort pass."""
    import os
    import signal
    import time
    pgid = proc.pid
    try:
        for sig in (signal.SIGTERM, signal.SIGKILL):
            try:
                os.killpg(pgid, sig)
            except ProcessLookupError:
                break                    # group verifiably empty
            except PermissionError:
                break                    # cannot signal: nothing more to do
            deadline = time.monotonic() + 3.0
            while time.monotonic() < deadline:
                try:
                    proc.wait(timeout=0.1)
                except Exception:
                    pass
                if not _group_alive(pgid):
                    break
            if not _group_alive(pgid):
                break
    finally:
        _close_pipes(proc)
        try:
            proc.wait(timeout=1)
        except Exception:
            pass


class SpawnRefused(RuntimeError):
    """The edit child was REFUSED, not failed, and NO live writer can
    remain: either the durable spawn intent could not be recorded (no
    child was ever spawned) or the spawned child's death - and its
    whole group's - was positively confirmed after the bind failure.
    Only then is the caller's retry-safe classification sound: the
    reserved output may be removed and the claim released."""


class SpawnUncertain(RuntimeError):
    """A writer WAS spawned but could not be bound, and its death could
    not be positively confirmed (best-effort kill returned with the
    child or its group possibly alive). The child may still be writing
    the reserved output: the caller must keep the claim (RECOVERING
    with its owner token), keep the output, and leave the spawn intent
    unresolved - fenced, no retry, no delete."""


def _confirm_dead(proc, child_start, proc_fs: str = "/proc") -> bool:
    """Positively confirm a spawned child AND its whole group are dead.
    Reaps the child (escalating to a direct SIGKILL once when a plain
    wait times out - _kill_group is best-effort only), then verifies
    the recorded (pid, start, pgid) identity against /proc, including
    a group-survivor scan. ANY ordinary failure of the confirmation
    channel (wait OSError, kill failure, unreadable birth, proof
    error) is uncertainty - False, never a propagated generic error.
    Cancellation (KeyboardInterrupt and other BaseExceptions) is
    never swallowed."""
    try:
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                proc.kill()
            except OSError:
                pass
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                return False
        if child_start is None:
            return False        # birth never readable: cannot prove
        return bool(claim_owner_dead(proc.pid, child_start, proc.pid,
                                     proc_fs)[0])
    except Exception:
        return False            # broken channel == unconfirmed


def edit_clip(ffmpeg: str, src: str, out: str, preset: OutputPreset,
              subtitle_path: str | None = None, focus_x: float = 0.5,
              runtime_dir: str = ".nova-runtime",
              watermark: str | None = None,
              pre_spawn=None, on_spawn=None,
              framing: str = "crop") -> Path:
    env = dict(os.environ)
    if subtitle_path or watermark:
        # drawtext and libass both resolve fonts through fontconfig;
        # relocated prefixes ship a fontconfig that points at paths
        # which no longer exist (device-verified on the owner's AGT phone)
        env.update(ensure_fontconfig_env(ffmpeg, runtime_dir))
    # two-phase durable registration (when hooks are given): intent
    # BEFORE spawn, identity bound right after; an unrecordable writer is
    # never spawned and an unbindable one is killed with its whole group,
    # because a surviving edit child can still write its output file
    token = pre_spawn() if pre_spawn is not None else None
    if pre_spawn is not None and token is None:
        raise SpawnRefused("edit spawn intent not recordable; not spawning")
    proc = subprocess.Popen(
        edit_args(ffmpeg, src, out, preset, subtitle_path, focus_x,
                  watermark=watermark, framing=framing),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env,
        start_new_session=True)
    if on_spawn is not None:
        try:
            bound = on_spawn(proc.pid, proc.pid, token)
        except Exception:
            bound = False
        if not bound:
            # the child must never run unrecorded; _kill_group is only
            # best-effort, so refusal requires POSITIVELY confirmed
            # death of the child and its whole group. Escalate with a
            # direct kill and reap, then verify against /proc: only a
            # confirmed-dead writer makes SpawnRefused retry-safe.
            try:
                child_start = read_start_time(proc.pid)
            except Exception:
                child_start = None    # unreadable birth: unprovable
            try:
                _kill_group(proc)
            except Exception:
                pass                  # best-effort by contract
            if not _confirm_dead(proc, child_start):
                _close_pipes(proc)
                raise SpawnUncertain(
                    "edit writer identity not recorded and child death"
                    " unconfirmed; fenced, not retry-safe")
            raise SpawnRefused(
                "edit writer identity not recorded; killed")
    try:
        try:
            out_s, err_s = proc.communicate(timeout=600)
        except subprocess.TimeoutExpired:
            _kill_group(proc)
            raise
    finally:
        _close_pipes(proc)
    if proc.returncode != 0:
        raise RuntimeError(f"edit failed rc={proc.returncode}: "
                           f"{err_s[-300:]!r}")
    return Path(out)
