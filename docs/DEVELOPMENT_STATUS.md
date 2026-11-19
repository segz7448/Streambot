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
| 007 | Telegram bot | LIVE but UNPAIRED: deployment nova-clip-bot running since 01:36 PST (PTB 22.8 vendored, sha256-verified wheels; token on-device via age-encrypted transfer, getMe OK both sides; $HOME-path crash loop found and fixed, first crashed deployment removed in owner's view). Pairing challenge expired unpaired 01:53 PST; awaiting owner action. Committed-not-rolled-out pending safe stop/start: log condenser, drop_pending_updates=False + watermark (acknowledged crash-gap limit: confirm-to-handle window open until a persistent pre-ack inbox exists), failed-set refinement + no-evict (overflow alerts, never silent suppression), AutoClipper + providers |
| 008 | Event detection | DEVICE-VERIFIED heuristics on agt-3f177cf6 (audio spikes at exact loudness transition, scene change t=10.0 score 0.806 with continuous-encode fixture; real-content threshold tuning pending; NOT AI understanding) |
| 009 | AI intelligence | CODE READY: provider interface with $0 heuristic default (never labeled AI) and explicit-config-only OpenAI-compatible adapter that refuses to default anything (a92f1d4); Groq recommended to owner with official-source retention/limits/ZDR facts and explicit signup consent pending. Quota exhaustion queues jobs and reports AI OFFLINE - no silent heuristic substitution |
| 010 | Automatic clipping | CODE READY + crash-hardened: AutoClipper consumes pending/queued events, scores via configured provider, extracts via real extractor, exactly-once clips rows (unique index + INSERT OR IGNORE; crash-window test proves retry cannot duplicate; 4fae05f). Live-source acceptance still pending Phase 006 |
| 011 | Video editing | DEVICE-VERIFIED on agt-3f177cf6 for the vertical preset (1080x1920 output probed from 320x240 source, loudnorm applied; subtitle burn pixel-proven 2026-11-19 after font-rendering fix, commit 7a87fe7); square/horizontal share the same graph construction, not separately device-run |
| 012 | Quality control | DEVICE-VERIFIED on agt-3f177cf6 (media verification passed on real edited output; duplicate detection confirmed both directions: derived file flagged, unrelated file not) |
| 013 | Telegram delivery | LIVE-ACCEPTED 2026-11-19: synthetic clip delivered to paired owner (Telegram message_id 15) after upload-timeout hardening (b0b0e96); owner-scoped by construction (paired admin only) |
| 014 | Live distribution (relay) | NOT STARTED (no code) |
| 015 | Monitoring & recovery | PARTIAL (supervisor + health + job recovery in foundation; bot recovery rollout live: restart-safe updates + watermark, no-evict failed set, condensed recoverable logs; confirm-to-handle crash gap documented, persistent pre-ack inbox not built) |
| 016 | Acceptance test | AUTOMATIC FIXTURE CHAIN VERIFIED 2026-11-19 02:43 on agt-3f177cf6: a real detected candidate (audio_spike, confidence 0.855, at the fixture's loud burst) drove extraction with no timestamp injection; record->detect->extract->edit(vertical)->QC->deliver completed in one supervised run (512,722-byte clip, QC pass, Telegram message_id 17 to the paired owner, clean stop). Fixture: h264/aac testsrc2 with a verified silence->burst transition (an earlier mpeg4 fixture exposed a real concat-decode incompatibility, fixed by using a representative h264 source). REAL-SOURCE acceptance still not run: awaits a verified authorized Twitch ingestion route. Fixture evidence never substitutes for it. |
| 017 | Documentation & release | README + this file started |

Added scope (owner, 2026-11-18): YouTube publishing module (OAuth 2.0,
private/unlisted by default, no browser automation, no publication claims
until YouTube confirms). STATUS: NOT STARTED (no code); gated on owner's
Google OAuth consent.

## Scope accuracy note (2026-11-19 02:15)

Corrected tiers: publisher and relay have no code and are not merely
"user-blocked" - they are unimplemented. Only Twitch ingestion (route
verification pending), AI provider (owner declined Groq; $0 on-device
feasibility under assessment), and YouTube OAuth are owner-decision
gated. Everything else unfinished is implementation work still owed.

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

## Phase 006/014 additions — ingestion route + distribution (2026-11-19, ~02:55 PST)

PUSHED (main@98e11d5, 123/123 unit tests) — code-complete tiers only, NOT
deployed to agt-3f177cf6 (live deployment e7a6c026 still runs b0b0e96).

- app/ingestion/twitch_clips.py: official Twitch Clips route. Create Clip
  (clips:edit, any live channel, authenticating user owns the clip),
  poll Get Clips, Get Clips Download (beta). VERIFIED LIMIT: download is
  reserved to the broadcaster or channel editors
  (channel:manage:clips / editor:manage:clips); the calling token's USER
  must hold that role - third-party OAuth alone confers nothing.
  Arbitrary-channel file pull and continuous capture are not officially
  deliverable. Status: implemented, NOT live accepted (gated on Twitch
  dev app + owner OAuth consent).
- app/distribution/relay.py: (a) Telegram clip forwarding to
  owner-registered destinations (0600 store, partial failures visible);
  (b) RTMP restream of an explicitly owner-provided feed to
  owner-configured RTMP endpoints - the requested RTMP scope, kept
  separate from (a); refuses with no feed or no destinations. Neither
  live-tested.
- app/distribution/youtube.py: YouTube Data API v3 publisher, verified
  against official docs: device flow requires a "TVs and Limited Input
  devices" OAuth client; its allowed scope list excludes youtube.upload
  but includes the broad youtube scope accepted by videos.insert
  (SCOPE constant corrected). Uploads via an API project that has not
  passed YouTube's compliance audit are locked private and unappealable
  (support.google.com/youtube/answer/7300965); module defaults to
  private. Unit-mocked only - mocks do not establish API compatibility;
  first live call is a gated step requiring owner OAuth consent.
