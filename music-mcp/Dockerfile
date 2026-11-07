FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY music_mcp ./music_mcp

RUN useradd --uid 65532 --no-create-home --shell /usr/sbin/nologin app \
 && mkdir -p /data && chown 65532:65532 /data
VOLUME /data
USER 65532

ENV DB_PATH=/data/music.db
EXPOSE 8000
CMD ["python", "-m", "music_mcp.server"]
