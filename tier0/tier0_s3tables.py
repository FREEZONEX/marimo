"""
Tier0 S3 Tables 预配置模块
用法: from tier0_s3tables import s3conn

该模块提供一个智能的 DuckDB 连接包装器，具有以下特性：
1. 自动使用 sitecustomize.py 初始化的连接
2. 根据凭证方法（credential_chain 或 explicit_boto3）提供不同的刷新策略
3. 在查询失败时自动尝试刷新凭证并重试
4. 支持长时间运行的 notebook 而不会因凭证过期失败

凭证刷新策略：
- credential_chain：DuckDB 自动处理，一般不需要手动刷新
- explicit_boto3：每 10 小时主动刷新一次（IRSA 默认 12 小时有效期）

环境变量:
- S3TABLES_BUCKET_ARN: S3 Tables bucket ARN
- NAMESPACE_ID: 命名空间 ID (通常等于 workspace ID)
- S3TABLES_DATABASE: S3 Tables 在 DuckDB 中的 database 名称 (默认 s3tables)
- AWS_REGION: AWS 区域 (默认 ap-southeast-1)
"""

import os
import sys
import time
import threading
import duckdb
import builtins

# 凭证刷新间隔（秒）- 仅用于 explicit_boto3 方法
# 设置为 10 小时，小于 IRSA 默认的 12 小时有效期
_CREDENTIAL_REFRESH_INTERVAL = 10 * 60 * 60  # 10 hours


class S3TablesConnection:
    """
    智能的 S3 Tables 连接包装器。

    特性：
    1. 如果使用 credential_chain，依赖 DuckDB 自动刷新凭证
    2. 如果使用 explicit_boto3，每 10 小时主动刷新凭证
    3. 在查询失败时自动尝试刷新凭证并重试（最多重试一次）
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection, credential_method: str = None):
        self._conn = conn
        self._credential_method = credential_method or "unknown"
        self._last_refresh = time.time()
        self._initialized = True  # sitecustomize 已经初始化

    def _refresh_credentials_boto3(self) -> bool:
        """使用 boto3 刷新 AWS 凭证（仅用于 explicit_boto3 方法）"""
        try:
            import boto3

            session = boto3.Session()
            credentials = session.get_credentials()
            if credentials is None:
                print("[tier0_s3tables] boto3 returned no credentials", file=sys.stderr)
                return False

            frozen = credentials.get_frozen_credentials()
            region = os.getenv("AWS_REGION", "ap-southeast-1")

            # 删除旧的 Secret 并创建新的
            try:
                self._conn.sql("DROP SECRET IF EXISTS aws_s3tables;")
            except Exception:
                pass

            self._conn.sql(f"""
                CREATE SECRET aws_s3tables (
                    TYPE S3,
                    KEY_ID '{frozen.access_key}',
                    SECRET '{frozen.secret_key}',
                    SESSION_TOKEN '{frozen.token}',
                    REGION '{region}'
                );
            """)

            self._last_refresh = time.time()
            print("[tier0_s3tables] Credentials refreshed (boto3)", file=sys.stderr)
            return True

        except Exception as e:
            print(
                f"[tier0_s3tables] Error refreshing credentials: {e}", file=sys.stderr
            )
            return False

    def _refresh_credentials_chain(self) -> bool:
        """重建 credential_chain Secret（用于 credential_chain 方法失败时）"""
        try:
            region = os.getenv("AWS_REGION", "ap-southeast-1")

            # 删除旧的 Secret 并创建新的
            try:
                self._conn.sql("DROP SECRET IF EXISTS aws_s3tables;")
            except Exception:
                pass

            self._conn.sql(f"""
                CREATE SECRET aws_s3tables (
                    TYPE S3,
                    PROVIDER credential_chain,
                    REGION '{region}'
                );
            """)

            self._last_refresh = time.time()
            print(
                "[tier0_s3tables] Credentials refreshed (credential_chain)",
                file=sys.stderr,
            )
            return True

        except Exception as e:
            print(
                f"[tier0_s3tables] Error refreshing credentials: {e}", file=sys.stderr
            )
            return False

    def _maybe_refresh_credentials(self):
        """如果使用 explicit_boto3 且凭证即将过期，主动刷新凭证"""
        if self._credential_method != "explicit_boto3":
            return  # credential_chain 由 DuckDB 自动处理

        elapsed = time.time() - self._last_refresh
        if elapsed > _CREDENTIAL_REFRESH_INTERVAL:
            print(
                f"[tier0_s3tables] Credentials expired ({elapsed / 3600:.1f}h), refreshing...",
                file=sys.stderr,
            )
            self._refresh_credentials_boto3()

    def _execute_with_retry(self, func, *args, **kwargs):
        """执行操作，失败时尝试刷新凭证并重试一次"""
        try:
            return func(*args, **kwargs)
        except Exception as e:
            error_str = str(e).lower()
            # 检查是否是凭证相关错误
            if any(
                keyword in error_str
                for keyword in [
                    "credential",
                    "access denied",
                    "forbidden",
                    "expired",
                    "authorization",
                ]
            ):
                print(
                    f"[tier0_s3tables] Query failed, attempting credential refresh: {e}",
                    file=sys.stderr,
                )

                # 根据凭证方法选择刷新策略
                refreshed = False
                if self._credential_method == "credential_chain":
                    refreshed = self._refresh_credentials_chain()
                else:
                    refreshed = self._refresh_credentials_boto3()

                if refreshed:
                    # 重试一次
                    return func(*args, **kwargs)

            # 不是凭证问题或刷新失败，重新抛出原始异常
            raise

    def sql(self, query: str):
        """执行 SQL 查询，自动处理凭证刷新"""
        self._maybe_refresh_credentials()
        return self._execute_with_retry(self._conn.sql, query)

    def execute(self, query: str, parameters=None):
        """执行 SQL 查询，自动处理凭证刷新"""
        self._maybe_refresh_credentials()
        return self._execute_with_retry(self._conn.execute, query, parameters)

    def __getattr__(self, name):
        """代理其他方法到底层连接"""
        return getattr(self._conn, name)


def _refresh_credentials_boto3(conn: duckdb.DuckDBPyConnection) -> bool:
    """使用 boto3 刷新 AWS 凭证（仅用于 explicit_boto3 方法）"""
    try:
        import boto3

        session = boto3.Session()
        credentials = session.get_credentials()
        if credentials is None:
            print("[tier0_s3tables] boto3 returned no credentials", file=sys.stderr)
            return False

        frozen = credentials.get_frozen_credentials()
        region = os.getenv("AWS_REGION", "ap-southeast-1")

        try:
            conn.sql("DROP SECRET IF EXISTS aws_s3tables;")
        except Exception:
            pass

        conn.sql(f"""
            CREATE SECRET aws_s3tables (
                TYPE S3,
                KEY_ID '{frozen.access_key}',
                SECRET '{frozen.secret_key}',
                SESSION_TOKEN '{frozen.token}',
                REGION '{region}'
            );
        """)

        print("[tier0_s3tables] Credentials refreshed (boto3)", file=sys.stderr)
        return True
    except Exception as e:
        print(f"[tier0_s3tables] Error refreshing credentials: {e}", file=sys.stderr)
        return False


def _start_background_refresh(
    conn: duckdb.DuckDBPyConnection, credential_method: str
) -> None:
    if credential_method != "explicit_boto3":
        return

    def _loop():
        while True:
            time.sleep(_CREDENTIAL_REFRESH_INTERVAL)
            _refresh_credentials_boto3(conn)

    thread = threading.Thread(target=_loop, daemon=True)
    thread.start()


def _attach_s3tables(conn: duckdb.DuckDBPyConnection) -> str | None:
    bucket_arn = os.getenv("S3TABLES_BUCKET_ARN")
    if not bucket_arn:
        return None

    region = os.getenv("AWS_REGION", "ap-southeast-1")
    namespace_id = os.getenv("NAMESPACE_ID")
    s3tables_database = os.getenv("S3TABLES_DATABASE", "s3tables")

    credential_method = None

    try:
        try:
            conn.sql("DROP SECRET IF EXISTS aws_s3tables;")
        except Exception:
            pass
        conn.sql(f"""
            CREATE SECRET aws_s3tables (
                TYPE S3,
                PROVIDER credential_chain,
                REGION '{region}'
            );
        """)
        credential_method = "credential_chain"
    except Exception as e:
        print(
            f"[tier0_s3tables] credential_chain failed: {e}, trying explicit credentials",
            file=sys.stderr,
        )

    if credential_method is None:
        try:
            import boto3

            session = boto3.Session()
            credentials = session.get_credentials()
            if credentials:
                frozen = credentials.get_frozen_credentials()
                try:
                    conn.sql("DROP SECRET IF EXISTS aws_s3tables;")
                except Exception:
                    pass
                conn.sql(f"""
                    CREATE SECRET aws_s3tables (
                        TYPE S3,
                        KEY_ID '{frozen.access_key}',
                        SECRET '{frozen.secret_key}',
                        SESSION_TOKEN '{frozen.token}',
                        REGION '{region}'
                    );
                """)
                credential_method = "explicit_boto3"
            else:
                print("[tier0_s3tables] boto3 returned no credentials", file=sys.stderr)
        except Exception as e:
            print(f"[tier0_s3tables] explicit credentials failed: {e}", file=sys.stderr)

    if credential_method is None:
        return None

    conn.sql(
        f"ATTACH '{bucket_arn}' AS {s3tables_database} (TYPE ICEBERG, ENDPOINT_TYPE s3_tables);"
    )

    if namespace_id:
        try:
            conn.sql(f'USE {s3tables_database}."{namespace_id}";')
        except Exception as e:
            print(
                f"[tier0_s3tables] Warning: Could not set default schema: {e}",
                file=sys.stderr,
            )

    print(
        f"[tier0_s3tables] S3 Tables attached (method={credential_method})",
        file=sys.stderr,
    )
    return credential_method


def _ensure_s3tables_attached(
    conn: duckdb.DuckDBPyConnection, credential_method: str
) -> str:
    try:
        tables = conn.sql("SHOW ALL TABLES").fetchall()
        if tables:
            return credential_method
    except Exception:
        pass

    method = _attach_s3tables(conn)
    if method is None:
        return credential_method

    if credential_method != method:
        print(
            f"[tier0_s3tables] Credential method updated: {credential_method} -> {method}",
            file=sys.stderr,
        )
    return method


def _create_connection():
    """创建 S3 Tables 连接"""
    # 优先使用 sitecustomize 创建的连接
    if hasattr(builtins, "_tier0_s3conn"):
        conn = builtins._tier0_s3conn
        credential_method = getattr(builtins, "_tier0_credential_method", "unknown")
        print(
            f"[tier0_s3tables] Using pre-initialized connection (method={credential_method})",
            file=sys.stderr,
        )
        credential_method = _ensure_s3tables_attached(conn, credential_method)
        _start_background_refresh(conn, credential_method)
        return conn, credential_method

    # sitecustomize 没有初始化，使用默认连接
    conn = duckdb.default_connection()
    print(
        "[tier0_s3tables] Warning: sitecustomize not initialized, using default connection",
        file=sys.stderr,
    )
    credential_method = _ensure_s3tables_attached(conn, "none")
    _start_background_refresh(conn, credential_method)
    return conn, credential_method


# 导出连接
_raw_conn, _credential_method = _create_connection()

# 1) 原生 duckdb 连接（marimo 侧边栏/SQL 插件识别）
s3conn = _raw_conn

# 2) 带重试封装的连接（保留给需要自动刷新/重试的代码）
s3conn_safe = S3TablesConnection(_raw_conn, _credential_method)
