#!/bin/sh
# Build the oxide bot image (no BuildKit needed). Run from anywhere; build on the Mac, then move it:
#   docker save aptbot-oxide:latest | ssh pi docker load
set -eu
cd "$(dirname "$0")/../.."
docker ${DOCKER_CONTEXT:+--context $DOCKER_CONTEXT} build --platform linux/arm64 -f deploy/aptbot-oxide/Dockerfile -t aptbot-oxide:latest .
