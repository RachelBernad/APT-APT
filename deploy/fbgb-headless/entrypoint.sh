#!/bin/sh
# Start a virtual display (+ window manager, + VNC if VNC=1), then run the command.
# Xvfb listens on no TCP port; VNC listens only inside the container (publish it as 127.0.0.1:5900 on the host).
set -eu
rm -f /tmp/.X99-lock /tmp/.X11-unix/X99 2>/dev/null || true
Xvfb "$DISPLAY" -screen 0 "$SCREEN" -nolisten tcp -ac >/tmp/xvfb.log 2>&1 &
for i in $(seq 1 50); do [ -e /tmp/.X11-unix/X99 ] && break; sleep 0.1; done
openbox >/tmp/openbox.log 2>&1 &
# Virtual sound card: a real desktop exposes at least an audio output (enumerateDevices() would be empty otherwise)
pulseaudio --start --exit-idle-time=-1 --disallow-exit >/tmp/pulse.log 2>&1 || true
pactl load-module module-null-sink sink_name=speakers sink_properties=device.description=Speakers >/dev/null 2>&1 || true
if [ "${VNC:-0}" = "1" ]; then
  # one-time random password per container (VNC uses only the first 8 chars); read it with:
  #   docker exec <container> cat /tmp/vncpass.txt
  head -c 64 /dev/urandom | tr -dc 'A-Za-z0-9' | head -c 8 > /tmp/vncpass.txt
  chmod 600 /tmp/vncpass.txt
  x11vnc -storepasswd "$(cat /tmp/vncpass.txt)" /tmp/vncpass >/dev/null 2>&1
  x11vnc -display "$DISPLAY" -forever -shared -rfbauth /tmp/vncpass -quiet -rfbport 5900 >/tmp/x11vnc.log 2>&1 &
fi
mkdir -p "$FBGB_HOME"
# A container that was killed leaves Chromium's profile lock behind, pointing at its old hostname. Chromium then
# refuses the profile ("in use by another computer") and hangs. One container per profile volume, so it's stale here.
rm -f "${FBGB_PROFILE_DIR:-$FBGB_HOME/profile}"/Singleton* 2>/dev/null || true
exec "$@"
