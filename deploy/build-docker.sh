#!/bin/sh
set -eu

# Works from any directory: build context is the repo root.
cd "$(dirname "$0")/.."

# Plain `docker build` for maximum compatibility (the Raspberry Pi runs an older
# Docker without the BuildKit --provenance/--sbom flags). On modern Docker this
# produces a slightly larger reported size due to attestation manifests, which is
# harmless; on the Pi it builds a clean single-manifest image.
docker build -f deploy/Dockerfile -t apt-apt-telegram-bot:latest .

docker images apt-apt-telegram-bot:latest --format 'Built {{.Repository}}:{{.Tag}}  size {{.Size}}'
