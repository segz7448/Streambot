#!/bin/sh
# NOVA Clip Engine - AGT ffmpeg installer.
# Downloads the pinned termux aarch64 package set (sha256-verified) and
# extracts it into an isolated prefix inside the app's home. It never touches
# AGT's runtime libraries and requires no root. Third-party binaries are
# fetched from the termux repository at install time (see their licenses);
# they are NOT committed to this repository.
set -eu
PREFIX_ROOT="${NOVA_FFMPEG_ROOT:-$HOME/instinct-clip}"
BASE="https://packages-cf.termux.dev/apt/termux-main"
cd "$PREFIX_ROOT" 2>/dev/null || mkdir -p "$PREFIX_ROOT/debs" "$PREFIX_ROOT/prefix" && cd "$PREFIX_ROOT"
mkdir -p debs prefix
MANIFEST="$(dirname "$0")/ffmpeg-manifest.txt"
fail=0
while read -r fn sha; do
    f=$(basename "$fn")
    [ -f "debs/$f.ok" ] && continue
    ok=0
    for try in 1 2 3; do
        if curl -sfL -m 300 -o "debs/$f" "$BASE/$fn" \
           && echo "$sha  debs/$f" | sha256sum -c - >/dev/null 2>&1; then
            touch "debs/$f.ok"; ok=1; break
        fi
        sleep 2
    done
    [ "$ok" -eq 1 ] || { echo "FAIL $f"; fail=1; }
done < "$MANIFEST"
[ "$fail" -eq 0 ] || { echo "download failures"; exit 1; }
for f in debs/*.deb; do dpkg-deb -X "$f" prefix; done
TP="$PREFIX_ROOT/prefix/data/data/com.termux/files/usr"
LD_LIBRARY_PATH="$TP/lib" "$TP/bin/ffmpeg" -hide_banner -version | head -1
echo "NOVA ffmpeg installed at $TP"
