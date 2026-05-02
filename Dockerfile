FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN groupadd -g 1000 app && useradd -u 1000 -g app -m app

COPY requirements.txt .
RUN python -m pip install --upgrade pip && \
    python -m pip install -r requirements.txt

COPY *.py ./

RUN mkdir -p /app/out && chown -R app:app /app

USER app

CMD ["python", "telegram_bot.py"]
