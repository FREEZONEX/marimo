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
- MARIMO_UV_TARGET: uv --target 安装目录，启动时自动注入 sys.path（替代 PYTHONPATH）
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

        # 禁用进度条：ATTACH S3 Tables 是网络调用，耗时 >2s 会触发 DuckDB 进度条写入 stdout，
        # 导致 uv pip list --format=json 的 JSON 输出被污染，packages 侧边栏显示 "No packages"
        conn.sql("SET enable_progress_bar = false;")

        # 安装并加载扩展（INSTALL 缓存命中时静默，已在 Dockerfile 以 appuser 预装到 /home/appuser/.duckdb/）
        conn.sql("INSTALL iceberg; INSTALL aws; LOAD iceberg; LOAD aws;")

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
    _early_init_s3tables()
    builtins._tier0_sitecustomize_done = True
