# NOVA Clip Engine — Development Status

Every phase lists actual evidence. "Done" means tested and verified, not
"the code was written". Nothing here is deployed to AGT yet.

| Phase | Scope | Status |
|-------|-------|--------|
| 001 | AGT environment audit | COMPLETE (verified live) |
| 002 | Project foundation | COMPLETE (tests pass locally) |
| 003 | FFmpeg engine | DEVICE-VERIFIED 2026-11-19 (evidence below); module integration continues |
| 004 | Media buffer | DEVICE-VERIFIED on agt-3f177cf6 (rolling capture: 8 realtime segments over 40s; storage 4,028,840 B under 4 MiB cap with total recorded content exceeding the cap, proving rotation; 6-segment event window retention; idempotent reconcile; clean supervised stop; harness commit 4496743). Live-source acceptance still pending Phase 006 |
| 005 | Twitch discovery | CODE READY (Helix client, tests 22/22); live acceptance needs user-approved Twitch app credentials |
| 006 | Authorized ingestion | NOT STARTED |
| 007 | Telegram bot | IN PROGRESS (bot token secured on-device 2026-11-19 via age-encrypted transfer to owner-verified phone key, getMe verified; framework integration next) |
| 008 | Event detection | DEVICE-VERIFIED heuristics on agt-3f177cf6 (audio spikes at exact loudness transition, scene change t=10.0 score 0.806 with continuous-encode fixture; real-content threshold tuning pending; NOT AI understanding) |
| 009 | AI intelligence | NOT STARTED |
| 010 | Automatic clipping | EXTRACTION device-proven (645KB clip from rolling buffer via module chain); automatic event-driven boundary selection pending Phase 009 |
| 011 | Video editing | DEVICE-VERIFIED on agt-3f177cf6 for the vertical preset (1080x1920 output probed from 320x240 source, loudnorm applied; subtitle burn pixel-proven 2026-11-19 after font-rendering fix, commit 7a87fe7); square/horizontal share the same graph construction, not separately device-run |
| 012 | Quality control | DEVICE-VERIFIED on agt-3f177cf6 (media verification passed on real edited output; duplicate detection confirmed both directions: derived file flagged, unrelated file not) |
| 013 | Telegram delivery | NOT STARTED |
| 014 | Live distribution | NOT STARTED |
| 015 | Monitoring & recovery | PARTIAL (supervisor + health + job recovery in foundation) |
| 016 | Acceptance test | NOT STARTED |
| 017 | Documentation & release | README + this file started |

Added scope (owner, 2026-11-18): YouTube publishing module (OAuth 2.0,
private/unlisted by default, no browser automation, no publication claims
until YouTube confirms).

## Subtitle font-rendering defect and fix (2026-11-19)

First on-device subtitle burn rendered unreadably (words touching, wrong
scale). Diagnosis on-device: the relocated Termux prefix ships a fontconfig
whose compiled-in paths point at the original Termux install location
("Fontconfig error: Cannot load default config file"), and the ASS style's
font alias "Sans" then resolved to a broken fallback with zero-width spaces.
Minimal on-device experiment ("AA BB CC DD", explicit DejaVu Sans) rendered
perfectly, isolating the cause. Fix (commit 7a87fe7): default_ass defaults
to the explicit "DejaVu Sans" shipped with the prefix, and
app/subtitles/fonts.py writes a minimal runtime fonts.conf pointing at the
prefix's real font directory plus /system/fonts, exported as FONTCONFIG_FILE
to the ffmpeg subprocess. Verified by pixel inspection of an extracted
output frame (caption wraps in three readable lines, spaces intact,
bottom safe area) rendered through the real edit_clip module on-device.
Caveat: still-frame proof of rendering, not subtitle sync.

## Secure bot-token transport (2026-11-19)

Telegram bot token transferred to the phone without plaintext in jobs,
logs, process args or git: encrypted locally with age v1.3.2 to the
phone-held key (public key re-verified live against the owner-verified
value from 2026-11-17), ciphertext-only in one signed job, decrypted
on-device to $HOME/instinct-clip/secrets/tg_token (mode 0600), ciphertext
removed. Phone-side getMe from the file succeeded; bot identity confirmed
distinct (@Zenasrobot). The token never transits chat or the repo.

## Phase 001 — AGT audit (2026-11-19 07:28-07:31 UTC)

Live signed command jobs against host agt-3f177cf6 (AGT 1.6.17). Full report:
[AGT_COMPATIBILITY.md](AGT_COMPATIBILITY.md). Headline: Python 3.12.10 IS
present (guide was written for 1.6.14); bionic environment, so glibc binaries
do not run; ffmpeg absent (Phase 003's key task); 3.6 GB RAM / 18.3 GB free
storage; ZAO BOT and other workloads untouched.

## Phase 003 — FFmpeg engine (2026-11-19)

On-device on host agt-3f177cf6: downloaded the pinned termux aarch64 ffmpeg
9.0.2 package set (103 packages, sha256-verified, zero failures) and extracted
it into the isolated prefix $HOME/instinct-clip/prefix (AGT runtime libs
untouched; nothing committed to git, install script fetches at deploy time).
Evidence jobs: download2.sh + f3f9c0a3 (extract), 9370c6e3 (ffmpeg -version:
9.0.2, libx264/aac/libass enabled), c0c3d525 (media battery: generate
testsrc2+sine -> h264/aac 10s OK; ffprobe OK; 5s cut OK; 1080x1920 9:16
crop/scale/pad OK; libass subtitle burn OK), dbe7523c (rolling segmentation:
3 keyframe-aligned segments from 16s input; note: -c copy splits on
keyframes, so buffer windows are approximate by design and finalized in
Phase 004 acceptance).

## Phase 002 — Foundation (2026-11-19)

Files created: full repository structure, app/core/{config,logging,lifecycle,
database,health,jobs}.py, app/storage schema migrations, app/main.py
entrypoint, packaging (.env.example, pyproject.toml, Dockerfile, scripts),
test suites.

Tests executed locally (Python 3.10, stricter than the phone's 3.12):
`python3 -m unittest discover -s tests -v` — 13 tests, all passing, including
an integration test that boots the app, migrates the database, serves
/health and shuts down cleanly on SIGTERM. One defect found and fixed during
the loop (job retry backoff was not simulated in the retry test).

Known limitations: no Telegram/Twitch/FFmpeg functionality yet; dependencies
must stay stdlib-only until pip bootstrap is verified on-device.

Remaining work: Phases 003-017 per the brief.
