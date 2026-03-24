# syntax=docker/dockerfile:1.12
# ============================================================
# FREEZONEX Tier0 Marimo 镜像（完整源码构建）
# ============================================================
#
# 完整从源码构建，支持前端和后端修改
#
# 构建命令:
#   cd D:\GoCoding\Tier0-marimo
#   docker build -f docker/Dockerfile.freezonex --target tier0 -t 350646758444.dkr.ecr.ap-southeast-1.amazonaws.com/tier0/tier0-marimo:test . --no-cache
#
# ============================================================

# ============================================================
# Stage 1: 构建前端
# ============================================================
FROM node:22-slim AS frontend-builder

# 安装 pnpm
RUN corepack enable && corepack prepare pnpm@latest --activate

WORKDIR /build

# 复制所有需要的文件（包括 marimo 源码中的 schema）
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml turbo.json tsconfig.json ./
COPY frontend ./frontend
COPY packages ./packages
COPY patches ./patches
# 复制 marimo 源码（前端构建需要 schema 文件）
COPY marimo ./marimo

# 安装依赖并构建前端
RUN pnpm install --frozen-lockfile
RUN pnpm turbo build --filter @marimo-team/frontend

# ============================================================
# Stage 2: Python 基础镜像
# ============================================================
FROM python:3.13-slim AS base

# 安装 uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# 创建非 root 用户
RUN useradd -m appuser

WORKDIR /app

# 复制 marimo 源码
COPY . /tmp/marimo-src

# 从前端构建阶段复制编译好的前端资源
COPY --from=frontend-builder /build/frontend/dist /tmp/marimo-src/marimo/_static

# 安装 marimo
RUN cd /tmp/marimo-src && \
    uv pip install --system . && \
    rm -rf /tmp/marimo-src

# 创建数据目录
RUN mkdir -p /app/data && chown -R appuser:appuser /app

# 环境变量
ENV MARIMO_SKIP_UPDATE_CHECK=1
ENV UV_SYSTEM_PYTHON=1
ENV PORT=8080
ENV HOST=0.0.0.0
EXPOSE $PORT

# ============================================================
# slim - 精简版
# ============================================================
FROM base AS slim
USER appuser
CMD ["marimo", "edit", "--no-token", "-p", "8080", "--host", "0.0.0.0"]

# ============================================================
# data - 数据分析版
# ============================================================
FROM base AS data
RUN uv pip install --system marimo[recommended,lsp] altair pandas numpy
USER appuser
CMD ["marimo", "edit", "--no-token", "-p", "8080", "--host", "0.0.0.0"]

# ============================================================
# sql - SQL 版
# ============================================================
FROM data AS sql
RUN uv pip install --system marimo[recommended,lsp,sql]
USER appuser
CMD ["marimo", "edit", "--no-token", "-p", "8080", "--host", "0.0.0.0"]

# ============================================================
# tier0 - Tier0 专用（PostgreSQL 直连方案）
# ============================================================
FROM sql AS tier0

USER root

# === S3 Tables 方案已封存，改用 PostgreSQL 直连 ===
# S3 Tables 核心依赖
# - duckdb, pyarrow, polars, sqlglot: S3 Tables / Iceberg 查询必需
# - boto3: 可选，供 notebook 用户在代码中使用 AWS SDK
# RUN uv pip install --system duckdb==1.4.4 boto3 pyarrow polars sqlglot

# PostgreSQL 直连依赖
# sqlalchemy: marimo 通过 DependencyManager 动态检测，不在 marimo[sql] 中，需显式安装
# psycopg2-binary: SQLAlchemy PostgreSQL 驱动
RUN uv pip install --system sqlalchemy psycopg2-binary

# 复制 Tier0 模块到 site-packages
COPY tier0/sitecustomize.py /tmp/sitecustomize.py
# === S3 Tables 模块已封存 ===
# COPY tier0/tier0_s3tables.py /tmp/tier0_s3tables.py
RUN SITE_PACKAGES=$(python -c "import site; print(site.getsitepackages()[0])") && \
    cp /tmp/sitecustomize.py "$SITE_PACKAGES/" && \
    rm /tmp/sitecustomize.py

# === DuckDB 扩展预下载已封存（S3 方案专用） ===
# USER appuser
# RUN python -c "import duckdb; duckdb.sql('INSTALL iceberg; INSTALL aws;'); print('Extensions installed')"

USER appuser

# 环境变量
ENV PYTHONUNBUFFERED=1
ENV PORT=2718
# uv 运行时包安装目标：隐藏目录（以 . 开头），marimo 文件浏览器跳过，同时在 PVC 内持久化
# 注意：不设置 PYTHONPATH（由 sitecustomize.py 在启动时动态注入 sys.path，避免污染子进程）
ENV MARIMO_UV_TARGET=/app/.packages
EXPOSE 2718

ENTRYPOINT ["marimo"]
CMD ["edit", "--no-token", "--host", "0.0.0.0", "-p", "2718"]
