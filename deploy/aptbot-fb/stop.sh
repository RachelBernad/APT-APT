#!/bin/sh
# Stop the bot; Chrome gets SIGTERM via the bot's shutdown (cookies are flushed).
set -eu
docker stop -t 30 aptbot-fb && docker rm aptbot-fb
