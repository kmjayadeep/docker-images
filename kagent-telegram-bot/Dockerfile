FROM python:3.15-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

RUN useradd --uid 65532 --no-create-home --shell /usr/sbin/nologin app
USER 65532

EXPOSE 8080
CMD ["python", "-I", "/app/app.py"]
