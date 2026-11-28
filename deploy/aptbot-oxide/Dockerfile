# The bot (Yad2 + Facebook groups) with Facebook read through BrowserOxide (FB_BROWSER=oxide): no Chromium,
# no Xvfb. ~450-560 MB for the FB worker, so it fits a Pi. Build from the REPO ROOT with deploy/aptbot-oxide/build.sh
# (plain multi-stage, works with the legacy builder). Build on a fast arm64 host (the Mac) and move the image to the
# Pi: the Rust build takes ~15-25 min and ~6 GB RAM.

# ---- builder: the browser_oxide wheel from the vendored fork (aptbot/browsers/oxide) ----
FROM python:3.12-slim AS oxide-build
ENV DEBIAN_FRONTEND=noninteractive PIP_NO_CACHE_DIR=1 CARGO_HOME=/opt/cargo RUSTUP_HOME=/opt/rustup PATH=/opt/cargo/bin:$PATH
RUN apt-get update && apt-get install -y --no-install-recommends build-essential curl ca-certificates cmake clang \
      libclang-dev pkg-config golang perl git libfreetype-dev libfontconfig-dev \
    && rm -rf /var/lib/apt/lists/*
RUN curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable
RUN pip install "maturin[patchelf]>=1.5,<2"
# Only the build inputs: KB/tools edits must not invalidate the ~20 min Rust layer.
WORKDIR /src
COPY aptbot/browsers/oxide/Cargo.toml aptbot/browsers/oxide/Cargo.lock aptbot/browsers/oxide/pyproject.toml ./
COPY aptbot/browsers/oxide/.cargo ./.cargo
COPY aptbot/browsers/oxide/python ./python
COPY aptbot/browsers/oxide/crates ./crates
RUN CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-4} maturin build --release -o /wheels

# ---- runtime ----
FROM python:3.12-slim
ENV DEBIAN_FRONTEND=noninteractive PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 TZ=Asia/Jerusalem LANG=C.UTF-8 \
    FBGB_HOME=/home/fbgb/.fbgb FB_BROWSER=oxide MALLOC_ARENA_MAX=2
# Fonts: oxide lays out and measures text (FB checks), so give it the same families Chrome on Linux has.
RUN apt-get update && apt-get install -y --no-install-recommends tzdata procps libfreetype6 libfontconfig1 \
      fonts-dejavu-core fonts-liberation fonts-noto-core fonts-culmus \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -m -u 1000 fbgb
COPY --from=oxide-build /wheels /tmp/wheels
COPY requirements.txt /tmp/requirements.txt
RUN pip install /tmp/wheels/*.whl -r /tmp/requirements.txt aiosqlite==0.22.1 && rm -rf /tmp/wheels /tmp/requirements.txt
WORKDIR /app
COPY aptbot ./aptbot
# The engine source stays in the builder; only the Python glue and docs are needed here.
RUN rm -rf /app/aptbot/browsers/oxide/crates /app/aptbot/browsers/oxide/Cargo.* \
    && mkdir -p /app/out /home/fbgb/.fbgb /home/fbgb/.aptbot \
    && chown -R fbgb:fbgb /app /home/fbgb && chmod 700 /home/fbgb/.fbgb /home/fbgb/.aptbot
# glibc static-TLS: the FB worker gets LD_PRELOAD of the native module from oxide/client.py (kb/caveats.md#linux-tls).
ENV PYTHONPATH=/app
USER fbgb
CMD ["python", "-m", "aptbot"]
