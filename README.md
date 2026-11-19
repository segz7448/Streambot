# NOVA Clip Engine

Automated Twitch live-stream clipping, AI video processing, Telegram-controlled
stream management and multi-platform distribution, designed to run on the AGT
Android hosting environment.

**Status: Phase 002 complete (foundation). Not yet deployed. See
[docs/DEVELOPMENT_STATUS.md](docs/DEVELOPMENT_STATUS.md) for verified,
phase-by-phase evidence.**

## What it does (target)

An administrator submits a Twitch broadcaster's username through a Telegram
bot. The engine resolves the channel, checks live status, ingests an
*authorized* video/audio feed, detects interesting moments, generates polished
short-form clips with subtitles, and delivers them through Telegram. Optional
live relay to authorized destinations and YouTube publishing (OAuth 2.0,
private/unlisted by default) are separate, independently controlled modules.

A Twitch username is a channel identifier, not a video-stream authorization
credential. This project never bypasses access controls, DRM or undocumented
playback URLs; ingestion and publishing require real authorization.

## Architecture

Pure-Python asyncio services under one supervisor (AGT ships Python 3.12,
bionic; no pip and no native wheels, so the core is stdlib-only):

```
app/
  bot/             Telegram controller (Phase 007)
  core/            config, logging, lifecycle, database, health, job queue
  ingestion/       authorized source adapters (Phase 006)
  recording/       FFmpeg recording + rolling buffer (Phases 003-004)
  intelligence/    event detection + AI inference adapters (Phases 008-009)
  clipping/        clip selection + extraction (Phase 010)
  editing/         presets, subtitles, QC (Phases 011-012)
  subtitles/       transcription + rendering (Phase 009/011)
  distribution/    live relay destinations (Phase 014)
  integrations/twitch/  Twitch Helix/EventSub client (Phase 005)
  ai_providers/    pluggable inference providers
  publishing/      YouTube upload pipeline (OAuth 2.0)
  api/             versioned HTTP API for future autonomous agents
  storage/         SQLite schema + migrations
  workers/         background workers
  monitoring/      supervision, recovery, storage management
  infrastructure/agt/   AGT deployment support
docs/              AGT compatibility report, development status
tests/             unit / integration / acceptance
```

## Quickstart (development)

Requires Python 3.10+ (3.12 on AGT). No third-party dependencies for the core.

```bash
cp .env.example .env     # fill in values; never commit .env
python3 -m app.main      # starts health endpoint on 127.0.0.1:8790
curl http://127.0.0.1:8790/health
python3 -m unittest discover -s tests -v
```

## Security rules (binding)

- No secrets in git, logs, job databases or process arguments. Tokens arrive
  through the environment or the AGT encrypted-transfer route only.
- Telegram bot commands are restricted to configured administrator IDs.
- YouTube/Twitch uploads default to private/unlisted until the administrator
  explicitly enables public publishing.
- Only material the administrator is authorized to use is ever ingested,
  clipped or published.

## License

MIT (see LICENSE).
