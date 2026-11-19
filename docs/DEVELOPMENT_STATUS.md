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

### YouTube OAuth route — RESOLVED to least privilege, owner consent pending (2026-11-19)

RECOMMENDED SUPPORTED ROUTE: Desktop app OAuth client + loopback
redirect + PKCE, scope youtube.upload ONLY. Google's Desktop Apps doc
explicitly covers "developer CLI tools, background sync utilities, or
scripts" (developers.google.com/identity/protocols/oauth2/native-app),
so this phone-hosted daemon fits that client type. The owner completes
consent in any browser on the SAME phone (he owns the Android host; the
daemon's earlier "no browser" assumption was wrong) - the redirect lands
on the daemon's 127.0.0.1 listener (app.distribution.youtube.LoopbackAuth,
implemented with PKCE S256 + state check, unit-tested incl. a real local
HTTP round-trip and state-mismatch refusal).

Device flow REJECTED: its allowed scope list excludes youtube.upload and
would force the broad youtube scope; a daemon is not the intended
limited-input device.

Constraints the owner must hear before consenting:
- Unverified project in Testing mode: owner must be a listed test user,
  sees an unverified-app warning, and the refresh token EXPIRES 7 DAYS
  after consent (weekly re-consent) until the project passes Google's
  sensitive-scope verification
  (support.google.com/cloud/answer/10311615).
- Uploads from a project that has not passed YouTube's compliance audit
  are locked private and unappealable
  (support.google.com/youtube/answer/7300965); the module defaults to
  private.
No authorization has been requested or taken; consent, when asked, names
the exact access (youtube.upload only).

## Deployment + hardening (2026-11-19, 03:04-03:47 PST)

Deployed build: e8387be on agt-3f177cf6, deployment id
f6dcf994-885e-4903-86e5-28c9b5501f19 (RUNNING). Deploy pulls main HEAD
at deploy time - the payload has no pin field (reproducibility gap,
noted). Earlier deployments stopped/removed; pairing + watermark state
survived both redeploys.

- On-device smoke (prefix Python 3.14): new-module suites 26/26 OK.
- rtmpfilter: rtmp(s) URLs reduced to scheme://host/<redacted> in all
  log records. Residual boundary documented: stream keys ride in ffmpeg
  argv, visible only to same-UID processes under Android sandbox;
  destinations live in 0600 state files, never in the repo.
- Pre-ack inbox (inbox.py + ops.drain_inbox): closes the
  confirm-to-handle gap - commands persisted BEFORE dispatch, removed
  after success, stranded entries replayed at startup, idempotent via
  watermark; failures stay for the next drain. 5 unit tests incl.
  crash-strand and next-boot-retry scenarios.
- CONTROLLED LOCAL RESTREAM LIFECYCLE - VERIFIED on device with real
  ffmpeg (25s h264/aac feed, two local .flv destinations, no real RTMP
  endpoint or stream keys involved): outputs grew 256KiB->512KiB while
  active; SIGKILL of one process is REAPED (a zombie-counting bug was
  found by this probe and fixed in e8387be: manager now poll()s its
  Popen handles / waitpid for foreign children); /stoprelay path
  SIGTERMs the survivor, which finalizes its file (832164 bytes);
  pid file clean afterwards. RTMP-network acceptance (a real streaming
  endpoint) remains untested - needs the owner's actual destination.

## Brief audit + hardening round 2 (2026-11-19, ~03:50 PST)

CODE: main@pending, 149/149. Deployed on phone: e8387be code (running
deployment f6dcf994); this round's changes are PUSHED-NOT-DEPLOYED.

BRIEF COMMAND AUDIT (section 4 of the brief): the full specified
surface is now registered - /start /help /addsource /sources
/removesource /checklive /startclip /stopclip /sessions /clips
/clipdetails /settings /quality /subtitles /relay /destinations
/stoprelay /status /storage /logs /restart (plus /clipnow for the
official trigger route). Behavior tiers: addsource/removesource,
sessions, clips, clipdetails, settings, quality, logs (0600 error ring
fed by handler exceptions) are functional against the state DB.
checklive/startclip/clipnow fail loudly while Twitch credentials are
unconfigured, per the brief's own rule (explain the missing
requirement, keep the source registered, never pretend recording
started). subtitles answers out-of-scope per the owner's decision.
restart answers honestly that worker restart is host-controlled on AGT.

GAP RESOLUTIONS:
- Secret-bearing RTMP DISABLED: /relay start and Relayer.restream
  refuse rtmp(s) URLs whose path carries a stream key (argv is readable
  by same-UID workloads on this shared host; no safe credential
  transport exists yet). Keyless destinations and local file outputs
  remain usable for testing.
- Inbox boundary corrected: the pre-ack inbox covers crashes AFTER
  handler entry; the framework queue window (server ack -> handler
  entry) is still uncovered; replay idempotence of external side
  effects (clip creation, restream start) is per-command and NOT yet
  implemented.

STILL MISSING / NOT ESTABLISHED:
- Continuous capture from a broadcaster-provided feed (mode A) and a
  retrospective local-recording command (mode C) are not implemented as
  commands.
- Gameplay-action selection/framing verified only on synthetic
  fixtures; HUD/action preservation on real gameplay UNACCEPTED.
- Live Twitch route, YouTube upload, and real RTMP acceptance each
  require owner-side setup/consent not yet in place.
- AI intelligence beyond audio-spike heuristics: no authorized $0
  provider.
