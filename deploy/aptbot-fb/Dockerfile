# The bot with BOTH sources (Yad2 + Facebook groups) in one container with no real display.
# Built on the fbgb-headless image (Chromium headful under Xvfb + openbox + PulseAudio, custom
# seccomp for the sandbox: see deploy/fbgb-headless/README.md). Build from the REPO ROOT with
# deploy/aptbot-fb/build.sh (it builds the base first; works with the legacy builder).
ARG BASE=fbgb-headless:latest
FROM ${BASE}

USER root
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt && rm /tmp/requirements.txt
COPY aptbot ./aptbot
RUN mkdir -p /app/out && chown -R fbgb:fbgb /app \
    && mkdir -p /home/fbgb/.aptbot && chown fbgb:fbgb /home/fbgb/.aptbot && chmod 700 /home/fbgb/.aptbot

USER fbgb
# The base entrypoint starts the virtual display, then runs this.
CMD ["python", "-m", "aptbot"]
