#!/bin/sh
# AGT deployment start command: must stay in the foreground.
# Secrets arrive via files placed through the encrypted-transfer route and
# are referenced by path, never passed as arguments.
set -eu
: "${NOVA_HOME:=$PWD}"
cd "$NOVA_HOME"
export NOVA_ENVIRONMENT=agt
exec python3 -m app.main
