#!/bin/sh
set -eu

# Plain `docker build` for maximum compatibility (the Raspberry Pi runs an older
# Docker without the BuildKit --provenance/--sbom flags). On modern Docker this
# produces a slightly larger reported size due to attestation manifests, which is
# harmless; on the Pi it builds a clean single-manifest image.
docker build -t apt-apt-telegram-bot:latest .

docker images apt-apt-telegram-bot:latest --format 'Built {{.Repository}}:{{.Tag}}  size {{.Size}}'
