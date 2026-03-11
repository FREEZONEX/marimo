"""
sitecustomize.py - Python 启动时自动执行

AK/SK 方案：使用长期 AWS 凭证（Access Key ID / Secret Access Key）
- 从环境变量读取 AWS_ACCESS_KEY_ID 和 AWS_SECRET_ACCESS_KEY
- 配置 DuckDB 静态 Secret，无需后台刷新线程
- 初始化 S3 Tables 连接供 Marimo 侧边栏和 tier0_s3tables 模块使用

环境变量:
- AWS_ACCESS_KEY_ID: AWS Access Key ID（必需）
- AWS_SECRET_ACCESS_KEY: AWS Secret Access Key（必需）
- AWS_REGION: AWS 区域（默认 ap-southeast-1）
- S3TABLES_BUCKET_ARN: S3 Tables bucket ARN（必需）
- NAMESPACE_ID: 命名空间 ID，通常等于 workspace ID
- S3TABLES_DATABASE: DuckDB 中的数据库名（默认 s3tables）
"""

import os
import sys
import builtins


def _early_init_s3tables():
    """在 Python 启动时初始化 S3 Tables 连接"""

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

        # 加载扩展（扩展已在 Dockerfile 中预装，直接 LOAD 避免 INSTALL 向 stdout 打印进度条）
        conn.sql("LOAD iceberg; LOAD aws;")

        region = os.getenv("AWS_REGION", "ap-southeast-1")
        namespace_id = os.getenv("NAMESPACE_ID")
        s3tables_database = os.getenv("S3TABLES_DATABASE", "s3tables")

        # 使用 AK/SK 创建静态 Secret（永不过期，无需刷新）
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

        # ATTACH S3 Tables
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

            # 获取当前 namespace 的表数量
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

        # 存储连接信息供 tier0_s3tables 使用
        builtins._tier0_s3conn = conn
        builtins._tier0_credential_method = "ak_sk"

    except Exception as e:
        import traceback

        print(
            f"[sitecustomize] Error initializing S3 Tables: {e}",
            file=sys.stderr,
        )
        traceback.print_exc()


# 确保只初始化一次
if not hasattr(builtins, "_tier0_sitecustomize_done"):
    _early_init_s3tables()
    builtins._tier0_sitecustomize_done = True
