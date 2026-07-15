# --- Builder stage: install deps into an isolated prefix ---
FROM python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
COPY requirements.txt .
# All dependencies ship prebuilt wheels for linux/amd64 + linux/arm64, so no
# compiler is needed. Install into /install for a clean copy into the runtime.
RUN pip install --prefix=/install -r requirements.txt

# --- Runtime stage: slim image with only the installed packages + source ---
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN groupadd -g 1000 app && useradd -u 1000 -g app -m app

COPY --from=builder /install /usr/local
COPY *.py ./
COPY data ./data

RUN mkdir -p /app/out && chown -R app:app /app

USER app

CMD ["python", "telegram_bot.py"]
