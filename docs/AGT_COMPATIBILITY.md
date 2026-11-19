# AGT Compatibility Report — NOVA Clip Engine Phase 001

Audit date: 2026-11-19 07:28-07:31 UTC (2026-11-18 ~23:30 PT)
Method: live signed command jobs against the repository owner's AGT phone host (AGT app 1.6.17) via the AGT job protocol. Read-only probes only; nothing installed or deployed.
Evidence job ids: 1727b0f1-ae38-4398-ad55-831997db6ea5 (identity), 3f1a768a-7e3f-4925-8560-9e008ba1973f (resources), 3dc51406-4078-4657-8edc-9a8d830eac63 (toolchain), c19415cd-133e-4f00-98d6-9505f841cd6f (storage/net), 9e6e054a-8843-4c5b-98b7-4ced94ddbb34 (python), 4e9a93f0-4867-4b15-a7cc-746a0bafb1e8 (usr/bin, process).

## 1. Platform identity
- Device: owner-authorized AGT phone host, app version 1.6.17 (guide documented 1.6.14 — the app has been updated and now ships Python).
- Kernel: Linux 4.19.157-perf aarch64 (Android 13, arm64-v8a, Redmi 10C).
- CPU: 8 cores (nproc reported 7 available).
- C library: Bionic (toybox 0.8.6-android; telemetry: "bash on bionic, no proot" for the shell layer; proot is used only for cloudflared). glibc/manylinux binaries will NOT run.
- Uptime at audit: ~51h; host online, tunnel CONNECTED, battery 77% (not charging; restarts pause below 25% unplugged).

## 2. Runtime toolchain (live probe, supersedes guide Section 7)
Present:
- Python 3.12.10 at /data/user/0/com.novainc.agt/files/usr/bin/python3 (stdlib sqlite3 3.46.1, asyncio, ssl via OpenSSL 3.0.15, ctypes; 56 lib-dynload modules; NO pip module, site-packages bare)
- git 2.51.0, curl 8.14.1 (aarch64-linux-android, OpenSSL/3.0.15), wget, tar, bash 5.2, busybox 1.36.1 (httpd, nc, crond, dpkg-deb, rpm2cpio present)
Missing:
- ffmpeg / ffprobe — must be sourced as a bionic-compatible ARM64 build (Phase 003 investigation; glibc static builds will not run; termux aarch64 .deb extraction via dpkg-deb is a candidate route)
- node, go, rustc, gcc/cc, sqlite3 CLI (Python's sqlite3 module covers DB needs), openssl CLI (Python ssl covers TLS)

## 3. Resources
- RAM: 3644 MB total, ~1594 MB available at audit, 4096 MB swap (~2541 MB free)
- Storage: /data 42.7 GB total, 18.3 GB free; current AGT deployments dir uses 18.4 MB
- Limits: 32768 open files, 13727 max processes, no CPU time cap, no root
- Outbound network works: api.twitch.tv reachable (401 without auth as expected), api.github.com 200 in ~3.2s
- Inbound: only via Cloudflare tunnel subdomains of the owner's tunnel domain for ported deployments (HTTP only on these subdomains per guide; HTTPS certificate coverage undecided)
- Background execution: AGT supervisor restarts deployments with backoff and restores after reboot; battery guard pauses below 25% unplugged; phone sleep/pause can interrupt check-ins (observed 11-17/18)

## 4. Current workloads (do not touch)
Running: several pre-existing owner workloads, including a Go-based Telegram
bot (verified live 11-18 09:45 PT; PRESERVE) and a pure-Python web service
on :8787 (proof pure-Python services run here). busybox httpd on :8080 also
running. All left untouched; names and ids withheld here intentionally.
Stopped: stale entries from the same workloads (no supported removal from list).
Ports observed in use: 8080, 8787.

## 5. Implications for NOVA Clip Engine
- Python 3.12 asyncio architecture is viable natively; dependencies must be pure-Python (no pip yet — bootstrap get-pip or vendor wheels; native wheels like pydantic-core, faster-whisper, numpy will NOT install on bionic without special builds).
- Telegram bot: pure-Python (httpx/python-telegram-bot vendored or pip via get-pip) feasible; a compiled Go controller is the proven fallback (a pre-existing owner bot runs as a Go binary).
- SQLite via Python stdlib: usable today.
- FFmpeg: the critical missing piece. Options ranked for Phase 003: (a) termux aarch64 .deb extracted with busybox dpkg-deb into the app prefix (bionic-matched), (b) upstream Android/bionic static ffmpeg build, (c) remote worker offload. Must be verified on-device before design locks.
- AI inference: local ML (whisper etc.) is not realistic on this CPU/RAM with bionic-only binaries for heavy models; remote inference adapters (configurable per brief) are the practical default, with local lightweight signal detection (audio levels via ffmpeg) on-device.
- Media storage: plan a configurable cap well under the 18.3 GB free; rolling buffer + cleanup mandatory.

## 6. Security observations
- A running bot deployment exposes its Telegram bot token in plaintext in the process list (curl polling command line). Same exposure class as the 11-17 incident. Reported to owner channel; not copied into this repo.
- Secrets must never enter jobs DB, git, logs, or process args; keep using the encrypted-transfer route established 11-17.

## 7. Open items carried to Phase 002/003
- pip bootstrap (get-pip.py) or vendored pure-Python deps — decide in Phase 002.
- FFmpeg bionic build acquisition and on-device verification — Phase 003, highest technical risk.
- Repo host/visibility decision (Streambot) — pending owner confirmation; AGT git-deploy requires a publicly cloneable repo.
