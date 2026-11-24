# Headful Chromium with NO real display (Xvfb), for facebook_generic_browser. The test image for the Raspberry Pi
# design (api_docs/stealth/pi-headless.md). Build from the REPO ROOT:
#   docker build -f deploy/fbgb-headless/Dockerfile -t fbgb-headless:test .
# Same base as the bot image (Debian 13 / python:3.12-slim), linux/arm64 like the Pi.
FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TZ=Asia/Jerusalem LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    DISPLAY=:99 SCREEN=1920x1080x24 \
    FBGB_HOME=/home/fbgb/.fbgb FBGB_BROWSER=/usr/bin/chromium \
    FBGB_CHROME_ARGS="--use-gl=angle --use-angle=gl --ignore-gpu-blocklist --start-maximized"
# FBGB_CHROME_ARGS (validated 2026-11-24, api_docs/stealth/pi-headless.md): WebGL via Mesa llvmpipe through Xvfb's
# GLX (looks like a Linux desktop without GPU drivers). Without it Chrome has NO WebGL at all, and SwiftShader
# (--enable-unsafe-swiftshader) is a known automation tell. Maximized window like a real user.
# Run with: --shm-size=1g --security-opt seccomp=deploy/fbgb-headless/seccomp-chromium.json

# chromium + its setuid sandbox helper, a virtual X server + a window manager, VNC for the one-time manual login,
# fonts a real Israeli desktop has (Hebrew: culmus/noto; emoji), tz + locale so the fingerprint matches the home IP.
RUN apt-get update && apt-get install -y --no-install-recommends \
        chromium chromium-sandbox xvfb xauth openbox x11vnc \
        fonts-noto-core fonts-noto-color-emoji fonts-culmus fonts-dejavu-core fonts-liberation \
        tzdata locales ca-certificates dbus-x11 procps pulseaudio \
    && sed -i 's/^# *en_US.UTF-8/en_US.UTF-8/; s/^# *he_IL.UTF-8/he_IL.UTF-8/' /etc/locale.gen && locale-gen \
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone \
    && rm -rf /var/lib/apt/lists/*

RUN pip install patchright==1.64.4 aiosqlite==0.22.1

RUN groupadd -g 1000 fbgb && useradd -u 1000 -g fbgb -m -s /bin/bash fbgb
# pre-create the home dir so a new named volume mounted here inherits fbgb ownership
RUN mkdir -p /home/fbgb/.fbgb && chown fbgb:fbgb /home/fbgb/.fbgb
# A desktop-like work area: openbox reserves a 32 px top bar (like GNOME), so screen.availHeight < screen.height
# (an "equal" pair is a headless/kiosk signal).
RUN mkdir -p /home/fbgb/.config/openbox \
    && sed 's|<margins>.*</margins>||; s|<top>0</top>|<top>32</top>|' /etc/xdg/openbox/rc.xml > /home/fbgb/.config/openbox/rc.xml \
    && chown -R fbgb:fbgb /home/fbgb/.config
WORKDIR /app
COPY aptbot/__init__.py aptbot/__init__.py
COPY aptbot/scrapers/__init__.py aptbot/scrapers/__init__.py
COPY aptbot/scrapers/facebook_generic_browser aptbot/scrapers/facebook_generic_browser
COPY deploy/fbgb-headless/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod 755 /usr/local/bin/entrypoint.sh && chown -R fbgb:fbgb /app

USER fbgb
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["python", "-m", "aptbot.scrapers.facebook_generic_browser", "status"]
