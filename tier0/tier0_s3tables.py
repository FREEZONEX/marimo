"""
Tier0 S3 Tables 预配置模块
用法: from tier0_s3tables import s3conn

该模块提供 DuckDB 连接，连接已由 sitecustomize.py 在启动时初始化。
使用 AK/SK 静态凭证，无需凭证刷新机制。

环境变量:
- S3TABLES_BUCKET_ARN: S3 Tables bucket ARN
- NAMESPACE_ID: 命名空间 ID (通常等于 workspace ID)
- S3TABLES_DATABASE: S3 Tables 在 DuckDB 中的 database 名称 (默认 s3tables)
- AWS_REGION: AWS 区域 (默认 ap-southeast-1)
- AWS_ACCESS_KEY_ID: AWS Access Key ID
- AWS_SECRET_ACCESS_KEY: AWS Secret Access Key
"""

import os
import sys
import builtins
import duckdb


def _attach_s3tables(conn: duckdb.DuckDBPyConnection) -> bool:
    """使用 AK/SK 配置 DuckDB 并 ATTACH S3 Tables"""
    bucket_arn = os.getenv("S3TABLES_BUCKET_ARN")
    if not bucket_arn:
        print(
            "[tier0_s3tables] S3TABLES_BUCKET_ARN not set",
            file=sys.stderr,
        )
        return False

    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    if not aws_access_key or not aws_secret_key:
        print(
            "[tier0_s3tables] AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY not set",
            file=sys.stderr,
        )
        return False

    region = os.getenv("AWS_REGION", "ap-southeast-1")
    namespace_id = os.getenv("NAMESPACE_ID")
    s3tables_database = os.getenv("S3TABLES_DATABASE", "s3tables")

    try:
        conn.sql("INSTALL iceberg; INSTALL aws; LOAD iceberg; LOAD aws;")
    except Exception:
        pass  # 可能已经安装/加载

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
                f"[tier0_s3tables] Warning: Could not set default schema: {e}",
                file=sys.stderr,
            )

    print(
        f"[tier0_s3tables] S3 Tables attached (method=ak_sk)",
        file=sys.stderr,
    )
    return True


def _create_connection() -> duckdb.DuckDBPyConnection:
    """获取或创建 S3 Tables 连接"""

    # 优先使用 sitecustomize 创建的连接
    if hasattr(builtins, "_tier0_s3conn"):
        conn = builtins._tier0_s3conn
        print(
            "[tier0_s3tables] Using pre-initialized connection (method=ak_sk)",
            file=sys.stderr,
        )

        # 验证连接是否正常
        try:
            conn.sql("SHOW ALL TABLES").fetchall()
            return conn
        except Exception:
            print(
                "[tier0_s3tables] Pre-initialized connection invalid, re-attaching",
                file=sys.stderr,
            )
            if _attach_s3tables(conn):
                return conn

    # sitecustomize 没有初始化，自行初始化
    conn = duckdb.default_connection()
    print(
        "[tier0_s3tables] Warning: sitecustomize not initialized, initializing now",
        file=sys.stderr,
    )
    if _attach_s3tables(conn):
        return conn

    # 返回未配置的默认连接（用户需自行处理）
    print(
        "[tier0_s3tables] Failed to attach S3 Tables, returning unconfigured connection",
        file=sys.stderr,
    )
    return conn


# 导出连接
s3conn = _create_connection()
