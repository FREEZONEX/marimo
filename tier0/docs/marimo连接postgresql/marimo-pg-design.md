# Marimo 直连 PostgreSQL 设计方案

## 1. 背景

当前 Marimo Notebook 通过 DuckDB + S3 Tables（AK/SK 凭证）读取数据。随着每位租户拥有专属 PostgreSQL 数据库，需要重构为 Marimo 直连 PostgreSQL，使租户能在 Notebook 中实时访问业务数据。

## 2. 目标

1. **租户 DB 无感直连**：Notebook 启动时自动注入 PG 连接凭证，无需用户手动配置
2. **表过滤与隔离**：仅展示 `uns` schema 下的动态业务表，隐藏所有底台初始化表
3. **S3 代码封存**：注释封存 S3 相关代码，不执行但保留源文件

## 3. 技术方案

采用 **marimo 原生 SQLAlchemy 引擎**（非 DuckDB postgres_scanner），理由：
- 直连 PG，零代理开销
- marimo 原生支持表发现、侧边栏展示、SQL Cell 执行
- 依赖更少，改动最小

### 3.1 改动范围

| 仓库 | 文件 | 改动类型 |
|------|------|----------|
| Tier0-Backend | `service/ressvr/internal/logic/notebookmanage/k8s/config.go` | 注释封存 S3 环境变量，新增 `DATABASE_URL`、`TIER0_VISIBLE_SCHEMA` |
| Tier0-Backend | `service/ressvr/internal/logic/notebookmanage/k8s/manager.go` | 注释封存 `createAWSCredentialsSecret()`/S3 ARN 注入，新增 `createPGCredentialsSecret()` |
| Tier0-Backend | `service/ressvr/internal/logic/notebookmanage/notebookInfoCreateLogic.go` | 查询 PG 记录并传入 DSN |
| Tier0-Backend | `service/ressvr/internal/logic/notebookmanage/helper.go` | 扩展 `buildK8sDeployConfig` 支持 `DatabaseURL` |
| Tier0-Backend | `service/ressvr/internal/domain/service/constants.go` | 注释封存 S3 常量，新增 PG Secret 名称常量 |
| Tier0-Backend | `service/ressvr/internal/svc/serviceContext.go` | 调整 `NewManager()` 调用（移除 `s3TablesARN` 参数） |
| Tier0-marimo | `tier0/sitecustomize.py` | 注释封存 S3 初始化，新增 PG 自动连接 |
| Tier0-marimo | `marimo/_runtime/runner/hooks_preparation.py` | 注册引擎注入钩子 |
| Tier0-marimo | `marimo/_runtime/runner/hooks_setup_engines.py` | 新建：从 builtins 注入预初始化引擎到 kernel globals |
| Tier0-marimo | `marimo/_sql/engines/sqlalchemy.py` | Schema 白名单过滤 |
| Tier0-marimo | `docker/Dockerfile` | 注释封存 S3 依赖，新增 `psycopg2-binary` |

### 3.2 Tier0-Backend 改动详情

#### 3.2.1 凭证注入流程

```
notebookInfoCreateLogic.NotebookInfoCreate()
  → 查询同 workspace 的 PostgreSQL 服务记录（service_type="postgres"）
  → 若不存在则返回明确错误（前置条件：PG 必须先于 Notebook 部署）
  → 解密 DBPassword（复用 svcCtx.DBPasswordCrypto.Decrypt()）
  → 构建 DATABASE_URL（postgresql:// scheme）
  → 传入 buildK8sDeployConfig() 的 DatabaseURL 字段
  → manager.Deploy() 中创建 pg-credentials Secret 并注入 Pod
```

#### 3.2.2 config.go 改动

**注释封存**的环境变量（S3 相关，保留代码但不执行）：
- `NAMESPACE_ID`
- `S3TABLES_DATABASE`
- `AWS_REGION`
- `AWS_DEFAULT_REGION`

**新增**的环境变量：
- `DATABASE_URL`：通过 K8s Secret 注入（非明文 env var），格式 `postgresql://tenant{id}:{password}@postgres.tenant-{id}.svc.cluster.local:5432/tenant{id}?sslmode=disable`
- `TIER0_VISIBLE_SCHEMA`：值为 `uns`，控制 marimo 侧边栏仅展示该 schema 的表

注意：DSN scheme 使用 `postgresql://`（SQLAlchemy 1.4+ 标准），而非现有 `BuildDSN()` 中的 `postgres://`。

**保留不变**的环境变量：
- `PORT`、`HOST`、`WORKSPACE_ID`、`MARIMO_SKIP_UPDATE_CHECK`、`MARIMO_CMD`、`UV`、`MARIMO_UV_TARGET`、`TZ`

**移除** `EnvFromSecrets` 中的 `AWSCredentialsSecretName`，替换为 `PGCredentialsSecretName`。

#### 3.2.3 manager.go 改动

**注释封存**：
- `s3TablesARN` 字段及其在 `Deploy()`/`UpdateFull()` 中的注入
- `createAWSCredentialsSecret()` 方法
- `AWSCredentialsSecretName` 常量

**新增**：
- `PGCredentialsSecretName = "pg-credentials"` 常量
- `createPGCredentialsSecret(ctx, namespace, databaseURL)` 方法：将 `DATABASE_URL` 写入 K8s Secret
- `Deploy()` 中调用 `createPGCredentialsSecret()` 替代 `createAWSCredentialsSecret()`
- `Undeploy()` 中清理 `pg-credentials` Secret 替代 `notebook-aws-credentials`

**`NewManager` 签名变更**：移除 `s3TablesARN string` 参数。

#### 3.2.4 NotebookConfig 扩展

`k8s.NotebookConfig` 新增 `DatabaseURL string` 字段。`NewConfigBuilder` 中将 `DATABASE_URL` 加入 `config.Env`。

#### 3.2.5 前置条件检查

`NotebookInfoCreate` 在构建部署配置前，查询 `res_workspace_service_info` 中 `service_type = "postgres"` 且 `workspace_id` 匹配的记录。若不存在，返回明确错误：`"PostgreSQL instance not found for this workspace, please provision PostgreSQL first"`。

这由 workspace 初始化流程保证（PG 在 Branch A 中先于 Notebook 部署），但显式检查可防御异常场景。

### 3.3 Tier0-marimo 改动详情

#### 3.3.1 sitecustomize.py

**注释封存**：`_early_init_s3tables()` 函数体完整保留，调用入口注释掉，附注释说明封存原因。

**新增** `_early_init_postgresql()` 函数：
```python
def _early_init_postgresql():
    """在 Python 启动时自动创建 PostgreSQL 连接"""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return

    try:
        from sqlalchemy import create_engine, text
        engine = create_engine(database_url, pool_pre_ping=True)

        # 验证连接可用
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        # 存入 builtins，供 marimo 内核 preparation hook 注入到 globals
        builtins._tier0_pg_engine = engine

        print(
            f"[sitecustomize] PostgreSQL initialized: pool_pre_ping=True",
            file=sys.stderr,
        )
    except Exception as e:
        import traceback
        print(
            f"[sitecustomize] Error initializing PostgreSQL: {e}",
            file=sys.stderr,
        )
        traceback.print_exc()
```

关键设计点：
- `pool_pre_ping=True`：自动重连（PG Pod 可能独立重启）
- 完整的 try-except 错误处理（与 S3 初始化风格一致）
- 即使连接测试失败也不阻塞 marimo 启动

**启动入口**调整：
```python
if not hasattr(builtins, "_tier0_sitecustomize_done"):
    _inject_uv_target_path()
    # _early_init_s3tables()  # S3 方案已封存，改用 PG 直连
    _early_init_postgresql()
    builtins._tier0_sitecustomize_done = True
```

#### 3.3.2 marimo 内核引擎注入

**问题**：marimo 的 `_broadcast_data_source_connection()` 仅扫描 cell 中定义的变量（`cell.defs`），不扫描 builtins。因此 `sitecustomize.py` 存入 builtins 的 engine 不会被自动发现。

**解决方案**：新增 marimo preparation hook，在首个 cell 执行前将 builtins 中的 engine 注入到 kernel globals。

**新建** `marimo/_runtime/runner/hooks_setup_engines.py`：
```python
"""Tier0: 注入 sitecustomize.py 预初始化的数据库引擎到 kernel globals"""
import builtins
from marimo._runtime.runner import cell_runner

def _inject_tier0_engines(runner: cell_runner.Runner) -> None:
    """将 sitecustomize.py 中预创建的 SQLAlchemy engine 注入 kernel globals，
    使 marimo 的 post-execution hook 能自动发现并在侧边栏展示。"""
    engine = getattr(builtins, "_tier0_pg_engine", None)
    if engine is not None and "pg" not in runner.glbls:
        runner.glbls["pg"] = engine
```

**修改** `marimo/_runtime/runner/hooks_preparation.py`：
在 `PREPARATION_HOOKS` 列表中注册 `_inject_tier0_engines`。

这样当任意 cell 执行后，post-execution hook 会扫描 globals 中的 `pg` 变量，检测到 SQLAlchemy Engine 类型，自动触发侧边栏数据源展示。

#### 3.3.3 Schema 过滤

**改动文件**：`marimo/_sql/engines/sqlalchemy.py`

在 `_get_schemas()` 方法开头，增加白名单过滤：

```python
def _get_schemas(self, *, database, include_tables, include_table_details):
    if self.inspector is None:
        return []
    try:
        schema_names = self.inspector.get_schema_names()
    except Exception:
        return []

    # Tier0: 环境变量白名单过滤，仅展示指定 schema
    visible_schema = os.getenv("TIER0_VISIBLE_SCHEMA")
    if visible_schema:
        schema_names = [s for s in schema_names if s == visible_schema]

    # ... 后续逻辑不变
```

逻辑：
- 若 `TIER0_VISIBLE_SCHEMA` 已设置（值为 `uns`），则 `schema_names` 仅保留匹配项
- 若未设置，沿用 marimo 默认行为（过滤 `information_schema`、`pg_catalog`）
- 白名单方式比黑名单更安全，天然排除 `public`、`timescaledb_*` 等所有非目标 schema

#### 3.3.4 Dockerfile 改动

**注释封存**（S3 相关，每段附注释说明）：
```dockerfile
# === S3 Tables 方案已封存，改用 PostgreSQL 直连 ===
# RUN uv pip install --system duckdb==1.4.4 boto3 pyarrow polars sqlglot
# COPY tier0/tier0_s3tables.py /tmp/tier0_s3tables.py
# RUN ... (tier0_s3tables.py COPY 到 site-packages)
# USER appuser
# RUN python -c "import duckdb; duckdb.sql('INSTALL iceberg; INSTALL aws;'); ..."
```

**新增**（PG 相关）：
```dockerfile
# PostgreSQL 直连依赖（sqlalchemy 已包含在 marimo[sql] 中）
RUN uv pip install --system psycopg2-binary
```

**保留不变**：
- `sitecustomize.py` 的 COPY（内容已改为 PG 路线）
- `MARIMO_UV_TARGET`、`PORT` 等环境变量
- `ENTRYPOINT` 和 `CMD`

### 3.4 K8s Secret 管理

PG 凭证通过 K8s Secret 注入 Notebook Pod：

- Secret 名称：`pg-credentials`（在 notebook 部署时由 `manager.Deploy()` 创建于 `tenant-{workspaceId}` namespace）
- Secret 数据：`DATABASE_URL` 字段
- Pod 通过 `envFrom.secretRef` 引用

注意：同 namespace 下已存在 PostgreSQL 部署创建的 `postgres-secret`（包含 `POSTGRES_PASSWORD`），但该 Secret 仅含密码不含完整 DSN，且由 postgres manager 管理。Notebook 使用独立的 `pg-credentials` Secret 保持生命周期解耦。

## 4. 数据表可见性规则

| Schema | 内容 | 可见性 |
|--------|------|--------|
| `uns` | 动态业务表（Metric/State/Action） | 展示 |
| `public` | 底台初始化表（`uns_*`、`sys_*` 约 21 张） | 隐藏 |
| `information_schema` | PG 系统目录 | 隐藏 |
| `pg_catalog` | PG 系统目录 | 隐藏 |
| `timescaledb_*` | TimescaleDB 内部 schema（4 个） | 隐藏 |
| `toolkit_experimental` | TimescaleDB toolkit | 隐藏 |

过滤由 `TIER0_VISIBLE_SCHEMA=uns` 环境变量 + `_get_schemas()` 白名单逻辑实现，一行代码覆盖所有隐藏需求。

## 5. 安全性

- PG 密码通过 K8s Secret 传递，不出现在 Deployment YAML 明文中
- 密码在 `res_workspace_service_info` 表中加密存储，部署时解密
- 每个租户独立数据库 + 独立凭证，天然隔离
- `DATABASE_URL` 使用 `sslmode=disable`（集群内网通信）

## 6. 向下兼容

- S3 方案代码（`_early_init_s3tables()`、`tier0_s3tables.py`）**源文件完整保留**
- Dockerfile 中 S3 相关步骤**注释封存**，附注释说明
- `config.go` 中 S3 环境变量设置代码**注释封存**
- `manager.go` 中 AWS 凭证相关代码**注释封存**
- `constants.go` 中 S3 常量**注释封存**
- 未来如需恢复 S3 方案，取消注释即可

## 7. 边界与容错

- **PG 未就绪**：`sitecustomize.py` 中 `pool_pre_ping=True` + `create_engine()` 延迟连接，即使 PG 暂时不可用，marimo 仍正常启动，后续查询时自动重连
- **PG 记录不存在**：`NotebookInfoCreate` 显式检查并返回错误，阻止创建无 PG 的 Notebook
- **PG Pod 重启**：SQLAlchemy 连接池自动重连，用户无感知
