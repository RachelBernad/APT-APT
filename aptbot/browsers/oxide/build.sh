#!/bin/sh
# Build the browser_oxide wheel from this vendored tree and install it into $OXIDE_PY's environment.
#   OXIDE_PY      python to install into (default: the research venv, else python3)
#   OXIDE_TARGET  cargo target dir, kept OUTSIDE the repo (default ~/.cache/aptbot-oxide-target)
#   OXIDE_LOG     build log (default $OXIDE_TARGET/build.log)
# Linux/arm64 (Pi image) builds happen in deploy/aptbot-oxide/Dockerfile (builder stage), not here.
set -e
HERE="$(cd "$(dirname "$0")" && pwd)"
[ -f "$HOME/.cargo/env" ] && . "$HOME/.cargo/env"
PY="${OXIDE_PY:-$HOME/aptbot-browser-research/browseroxide/venv/bin/python}"; [ -x "$PY" ] || PY=python3
TARGET="${OXIDE_TARGET:-$HOME/.cache/aptbot-oxide-target}"; mkdir -p "$TARGET"
LOG="${OXIDE_LOG:-$TARGET/build.log}"
cd "$HERE"
CARGO_TARGET_DIR="$TARGET" CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-4}" "$PY" -m maturin build --release -i "$PY" -o "$TARGET/wheels" > "$LOG" 2>&1 \
  || { grep -E "^error" -A12 "$LOG" | head -60; exit 1; }
WHEEL=$(ls -t "$TARGET"/wheels/browser_oxide-*.whl | head -1)
if command -v uv >/dev/null; then uv pip install --python "$PY" --reinstall --no-deps -q "$WHEEL"; else "$PY" -m pip install --force-reinstall --no-deps -q "$WHEEL"; fi
echo "installed $(basename "$WHEEL")"
