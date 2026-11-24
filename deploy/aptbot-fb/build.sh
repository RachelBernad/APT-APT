#!/bin/sh
# Build the base (fbgb-headless) and the bot image on top of it. Plain `docker build`
# (no BuildKit needed). Run from anywhere.
set -eu
cd "$(dirname "$0")/../.."
docker build -f deploy/fbgb-headless/Dockerfile -t fbgb-headless:latest .
docker build -f deploy/aptbot-fb/Dockerfile -t aptbot-fb:latest .
