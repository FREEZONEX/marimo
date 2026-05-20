# syntax=docker/dockerfile:1.12
# ============================================================
# FREEZONEX Tier0 Marimo image, built from the full local source tree.
# ============================================================

FROM node:22-slim AS frontend-builder

RUN corepack enable && corepack prepare pnpm@latest --activate

WORKDIR /build

COPY package.json pnpm-lock.yaml pnpm-workspace.yaml turbo.json tsconfig.json .oxfmtrc.json ./
COPY frontend ./frontend
COPY packages ./packages
COPY patches ./patches
COPY marimo ./marimo

RUN pnpm install --frozen-lockfile
RUN pnpm turbo build --filter @marimo-team/frontend

FROM python:3.13-slim AS base

LABEL org.opencontainers.image.source="https://github.com/marimo-team/marimo"
LABEL org.opencontainers.image.description="marimo reactive notebook"

COPY --from=ghcr.io/astral-sh/uv:0.10.9 /uv /uvx /bin/

RUN useradd -m appuser

WORKDIR /app

ARG marimo_version=0.12.8
LABEL org.opencontainers.image.version="${marimo_version}"

COPY . /tmp/marimo-src
COPY --from=frontend-builder /build/frontend/dist /tmp/marimo-src/marimo/_static

RUN cd /tmp/marimo-src && \
    uv pip install --system . && \
    rm -rf /tmp/marimo-src

RUN mkdir -p /app/data && \
    chown -R appuser:appuser /app && \
    chown -R appuser:appuser /usr/local/lib/python3.13/site-packages/ && \
    chown -R appuser:appuser /usr/local/bin/

ENV MARIMO_SKIP_UPDATE_CHECK=1
ENV UV_SYSTEM_PYTHON=1
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV PORT=8080
ENV HOST=0.0.0.0
EXPOSE $PORT

STOPSIGNAL SIGTERM

FROM base AS slim
USER appuser
CMD ["sh", "-c", "exec marimo edit --no-token -p $PORT --host $HOST"]

FROM base AS data
RUN uv pip install --no-cache-dir \
    altair \
    marimo[recommended,lsp] \
    numpy \
    pandas
USER appuser
CMD ["sh", "-c", "exec marimo edit --no-token -p $PORT --host $HOST"]

FROM data AS sql
USER root
RUN uv pip install --no-cache-dir marimo[sql]
USER appuser
CMD ["sh", "-c", "exec marimo edit --no-token -p $PORT --host $HOST"]

FROM sql AS tier0

USER root

# PostgreSQL direct connection support for Tier0.
RUN uv pip install --no-cache-dir sqlalchemy psycopg2-binary

COPY tier0/sitecustomize.py /tmp/sitecustomize.py
RUN SITE_PACKAGES=$(python -c "import site; print(site.getsitepackages()[0])") && \
    cp /tmp/sitecustomize.py "$SITE_PACKAGES/" && \
    rm /tmp/sitecustomize.py

USER appuser

ENV PYTHONUNBUFFERED=1
ENV PORT=2718
ENV MARIMO_UV_TARGET=/app/.packages
EXPOSE 2718

CMD ["sh", "-c", "exec marimo edit --no-token --host $HOST -p $PORT"]
