#!/bin/bash
# Fail-closed test gate. Every push goes through this: it runs the full
# suite and REFUSES to push unless the suite is green. Never bypass it.
set -euo pipefail
cd "$(dirname "$0")/.."
if ! python3 -m unittest discover -s tests -q; then
  echo "GATE: test suite failed - push REFUSED" >&2
  exit 1
fi
GIT_ASKPASS=/tmp/agt/askpass.sh GIT_TERMINAL_PROMPT=0 \
  git push https://github.com/segz7448/Streambot.git main:main
