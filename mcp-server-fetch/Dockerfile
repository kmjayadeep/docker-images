FROM python:3.15-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1

# Reference MCP fetch server (modelcontextprotocol/servers), pinned.
RUN pip install --no-cache-dir mcp-server-fetch==2026.10.24

RUN useradd --uid 65532 --no-create-home --shell /usr/sbin/nologin app
USER 65532

# stdio transport; kmcp wraps it with its HTTP adapter.
ENTRYPOINT ["mcp-server-fetch"]
