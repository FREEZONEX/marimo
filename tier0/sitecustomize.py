"""
sitecustomize.py - Python 启动时自动执行

PostgreSQL 直连方案：通过 DATABASE_URL 环境变量自动创建 SQLAlchemy Engine，
供 marimo 内核自动发现并在侧边栏展示租户数据库表。

环境变量:
- DATABASE_URL: PostgreSQL 连接串（必需，格式 postgresql://user:pass@host:5432/dbname）
- TIER0_VISIBLE_SCHEMA: 可见 schema 白名单（默认 uns）
- MARIMO_UV_TARGET: uv --target 安装目录，启动时自动注入 sys.path
"""

import os
import sys
import builtins


# === S3 Tables 方案已封存，改用 PostgreSQL 直连 ===
# 以下函数体完整保留，调用入口已注释。如需恢复 S3 方案，取消下方启动入口的注释即可。
def _early_init_s3tables():
    """在 Python 启动时初始化 S3 Tables 连接（已封存）"""

    bucket_arn = os.getenv("S3TABLES_BUCKET_ARN")
    if not bucket_arn:
        return

    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    if not aws_access_key or not aws_secret_key:
        print(
            "[sitecustomize] AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY not set, skipping S3 Tables init",
            file=sys.stderr,
        )
        return

    try:
        import duckdb

        conn = duckdb.default_connection()

        conn.sql("SET enable_progress_bar = false;")

        conn.sql("INSTALL iceberg; INSTALL aws; LOAD iceberg; LOAD aws;")

        region = os.getenv("AWS_REGION", "ap-southeast-1")
        namespace_id = os.getenv("NAMESPACE_ID")
        s3tables_database = os.getenv("S3TABLES_DATABASE", "s3tables")

        try:
            conn.sql("DROP SECRET IF EXISTS aws_s3tables;")
        except Exception:
            pass

        conn.sql(f"""
            CREATE SECRET aws_s3tables (
                TYPE S3,
                KEY_ID '{aws_access_key}',
                SECRET '{aws_secret_key}',
                REGION '{region}'
            );
        """)

        conn.sql(
            f"ATTACH '{bucket_arn}' AS {s3tables_database} (TYPE ICEBERG, ENDPOINT_TYPE s3_tables);"
        )

        if namespace_id:
            try:
                conn.sql(f'USE {s3tables_database}."{namespace_id}";')
            except Exception as e:
                print(
                    f"[sitecustomize] Warning: Could not set default schema: {e}",
                    file=sys.stderr,
                )

            all_tables = conn.sql("SHOW ALL TABLES").fetchall()
            namespace_tables = [
                row[2]
                for row in all_tables
                if row[0] == s3tables_database and row[1] == namespace_id
            ]

            print(
                f"[sitecustomize] S3 Tables initialized: method=ak_sk, "
                f"namespace={namespace_id}, tables={len(namespace_tables)}",
                file=sys.stderr,
            )
        else:
            print(
                f"[sitecustomize] S3 Tables initialized: method=ak_sk, "
                "NAMESPACE_ID not set",
                file=sys.stderr,
            )

        builtins._tier0_s3conn = conn
        builtins._tier0_credential_method = "ak_sk"

    except Exception as e:
        import traceback

        print(
            f"[sitecustomize] Error initializing S3 Tables: {e}",
            file=sys.stderr,
        )
        traceback.print_exc()
# === S3 Tables 封存结束 ===


def _early_init_postgresql():
    """在 Python 启动时自动创建 PostgreSQL 连接

    读取 DATABASE_URL 环境变量，创建 SQLAlchemy Engine 并存入 builtins，
    供 marimo 内核 preparation hook 注入到 globals 实现侧边栏自动发现。
    """
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return

    try:
        from sqlalchemy import create_engine, text

        engine = create_engine(database_url, pool_pre_ping=True)

        # 立即存入 builtins，pool_pre_ping 保证后续自动重连
        # 不以连接测试结果为前提，确保 PG 暂时不可用时 marimo 仍正常启动
        builtins._tier0_pg_engine = engine

        # 连接测试仅用于日志确认，不阻塞启动
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print(
                "[sitecustomize] PostgreSQL initialized and verified: pool_pre_ping=True",
                file=sys.stderr,
            )
        except Exception as e:
            print(
                f"[sitecustomize] PostgreSQL engine created but connectivity check failed (will auto-reconnect): {e}",
                file=sys.stderr,
            )
    except Exception as e:
        import traceback

        print(
            f"[sitecustomize] Error initializing PostgreSQL: {e}",
            file=sys.stderr,
        )
        traceback.print_exc()


def _inject_uv_target_path():
    """将 MARIMO_UV_TARGET 注入 sys.path，使运行时安装的包可被 Python 找到。

    使用此方式替代 PYTHONPATH 环境变量，原因：
    - PYTHONPATH 会被 uv 子进程继承，当目录不存在时 uv pip list 返回空列表
    - sys.path 只影响当前 Python 进程，不传递给子进程
    - 目录不存在时安全跳过，首次 uv install 后自动生效
    """
    uv_target = os.environ.get("MARIMO_UV_TARGET")
    if uv_target and uv_target not in sys.path:
        sys.path.insert(0, uv_target)


# 确保只初始化一次
if not hasattr(builtins, "_tier0_sitecustomize_done"):
    _inject_uv_target_path()
    # _early_init_s3tables()  # S3 方案已封存，改用 PG 直连
    _early_init_postgresql()
    builtins._tier0_sitecustomize_done = True
