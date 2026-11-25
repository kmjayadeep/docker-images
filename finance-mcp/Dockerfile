# beancount 3.2.3 has wheels up to CPython 3.14 only; on 3.15 pip compiles it and the slim image has no
# compiler (every build failed from 2026-11-09). renovate.json holds this image below 3.15.
FROM python:3.14-slim

# The ledger mount is read-only, so Beancount must not try to write its pickle cache next to it.
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1 BEANCOUNT_DISABLE_LOAD_CACHE=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY finance_mcp ./finance_mcp

RUN useradd --uid 65532 --no-create-home --shell /usr/sbin/nologin app
USER 65532

EXPOSE 8000
CMD ["python", "-m", "finance_mcp.server"]
