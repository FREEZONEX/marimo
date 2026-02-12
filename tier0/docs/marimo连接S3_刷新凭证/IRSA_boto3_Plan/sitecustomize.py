"""
sitecustomize.py - Python 启动时自动执行

方案4专用：配合 Marimo 源码 Patch 使用
- 初始化 S3 Tables 连接（供 Marimo 侧边栏显示表列表）
- 设置默认 schema 为当前 namespace
- Marimo 的 Patch 会自动过滤其他 namespace 的表

凭证策略：
1. 优先使用 credential_chain（DuckDB 自动刷新凭证，最可靠）
2. 如果 credential_chain 失败，fallback 到显式凭证（boto3）
3. tier0_s3tables.py 提供额外的凭证监控和刷新机制
"""

import os
import sys
import threading
import time
import builtins


def _early_init_s3tables():
    """在 Python 启动时初始化 S3 Tables 连接"""

    bucket_arn = os.getenv("S3TABLES_BUCKET_ARN")
    if not bucket_arn:
        return

    try:
        import duckdb

        # 使用 DuckDB 的默认连接
        conn = duckdb.default_connection()

        # 安装和加载扩展
        conn.sql("INSTALL iceberg; INSTALL aws; LOAD iceberg; LOAD aws;")

        region = os.getenv("AWS_REGION", "ap-southeast-1")
        namespace_id = os.getenv("NAMESPACE_ID")
        s3tables_database = os.getenv("S3TABLES_DATABASE", "s3tables")

        # 尝试创建 AWS Secret
        secret_created = False
        credential_method = None
        credential_refresh_interval = 10 * 60 * 60  # 10 hours

        def _refresh_credentials_boto3() -> bool:
            """使用 boto3 刷新 AWS 凭证（仅用于 explicit_boto3 方法）"""
            try:
                import boto3

                session = boto3.Session()
                credentials = session.get_credentials()
                if credentials is None:
                    print(
                        "[sitecustomize] boto3 returned no credentials",
                        file=sys.stderr,
                    )
                    return False

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

                print(
                    "[sitecustomize] Credentials refreshed (boto3)",
                    file=sys.stderr,
                )
                return True
            except Exception as e:
                print(
                    f"[sitecustomize] Error refreshing credentials: {e}",
                    file=sys.stderr,
                )
                return False

        def _start_background_refresh():
            def _loop():
                while True:
                    time.sleep(credential_refresh_interval)
                    _refresh_credentials_boto3()

            thread = threading.Thread(target=_loop, daemon=True)
            thread.start()
            builtins._tier0_refresh_thread_started = True

        # 方法1：优先使用 credential_chain（DuckDB 自动刷新凭证）
        try:
            try:
                conn.sql("DROP SECRET IF EXISTS aws_s3tables;")
            except Exception:
                pass
            conn.sql(f"""
                CREATE SECRET aws_s3tables (
                    TYPE S3,
                    PROVIDER credential_chain,
                    CHAIN 'sts;config;env;web_identity',
                    REGION '{region}'
                );
            """)
            secret_created = True
            credential_method = "credential_chain"
        except Exception as e:
            print(
                f"[sitecustomize] credential_chain failed: {e}, trying explicit credentials",
                file=sys.stderr,
            )

        # 方法2：fallback 到显式凭证（boto3）
        if not secret_created:
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
                    secret_created = True
                    credential_method = "explicit_boto3"
                    _start_background_refresh()
                else:
                    print(
                        "[sitecustomize] boto3 returned no credentials",
                        file=sys.stderr,
                    )
            except Exception as e:
                print(
                    f"[sitecustomize] explicit credentials failed: {e}",
                    file=sys.stderr,
                )

        if not secret_created:
            print(
                "[sitecustomize] Failed to create AWS secret with any method",
                file=sys.stderr,
            )
            return

        # ATTACH S3 Tables
        conn.sql(
            f"ATTACH '{bucket_arn}' AS {s3tables_database} (TYPE ICEBERG, ENDPOINT_TYPE s3_tables);"
        )

        if namespace_id:
            # 设置默认 schema 为当前 namespace
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
                f"[sitecustomize] S3 Tables initialized: method={credential_method}, "
                f"namespace={namespace_id}, tables={len(namespace_tables)}",
                file=sys.stderr,
            )
        else:
            print(
                f"[sitecustomize] S3 Tables initialized: method={credential_method}, "
                "NAMESPACE_ID not set",
                file=sys.stderr,
            )

        # 存储连接和凭证方法供 tier0_s3tables 使用
        import builtins

        builtins._tier0_s3conn = conn
        builtins._tier0_credential_method = credential_method

    except Exception as e:
        import traceback

        print(
            f"[sitecustomize] Error initializing S3 Tables: {e}",
            file=sys.stderr,
        )
        traceback.print_exc()


# 确保只初始化一次
if hasattr(builtins, "_tier0_sitecustomize_done"):
    pass
else:
    # 执行初始化
    _early_init_s3tables()
    builtins._tier0_sitecustomize_done = True
