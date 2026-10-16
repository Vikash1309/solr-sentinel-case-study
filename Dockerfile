# syntax=docker/dockerfile:1
# Targets:
#   production (default)  docker build -t solr-sentinel .
#   test                  docker build --target test -t solr-sentinel-test . && docker run --rm solr-sentinel-test

# Base image pinned by digest (python 3.12.15-slim) so builds are reproducible. Bump deliberately.
ARG PYTHON_IMAGE=python:3.12-slim@sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269512df273a0bb1784edc11b1

FROM ${PYTHON_IMAGE} AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_ROOT_USER_ACTION=ignore HOME=/home/app \
    SENTINEL_DB_PATH=/data/sentinel.db SENTINEL_UPLOAD_DIR=/data/uploads
# Unprivileged user. /data is the only directory the app writes to.
RUN groupadd --system --gid 10001 app \
 && useradd --system --uid 10001 --gid app --home-dir /home/app --create-home --shell /usr/sbin/nologin app \
 && mkdir -p /data && chown app:app /data
WORKDIR /app
# Exact versions with hashes: a tampered or unexpected package fails the build.
COPY requirements.lock .
RUN pip install --require-hashes -r requirements.lock
COPY run.py ./
COPY app ./app
COPY dashboard ./dashboard
COPY scripts ./scripts
COPY eval/ground_truth.json ./eval/
# Demo data only. In production, mount real logs and set SENTINEL_LOG_DIR.
RUN python scripts/generate_synthetic_logs.py --out sample_data/logs
EXPOSE 8000 8501

FROM runtime AS test
COPY requirements-dev.lock .
RUN pip install --require-hashes -r requirements-dev.lock
COPY pyproject.toml ./
COPY tests ./tests
# the evaluation writes its report and results here
RUN mkdir -p docs eval/results && chown -R app:app docs eval/results
USER app
CMD ["python", "-m", "pytest", "-q", "-p", "no:cacheprovider"]

FROM runtime AS production
USER app
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=4)"]
# The dashboard service overrides this command (see docker-compose.yml).
CMD ["uvicorn", "--factory", "app.main:create_app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers", "--no-server-header"]
