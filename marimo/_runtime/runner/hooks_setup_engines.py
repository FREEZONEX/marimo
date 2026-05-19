# Copyright 2026 Marimo. All rights reserved.
"""Tier0: 注入 sitecustomize.py 预初始化的数据库引擎到 kernel globals。

sitecustomize.py 在 Python 进程启动时创建 SQLAlchemy Engine 并存入 builtins，
但 marimo 的 post-execution hook（_broadcast_data_source_connection）仅扫描
cell.defs 中的变量，不会发现通过 globals 注入的引擎。

因此需要两个钩子配合：
- preparation hook：将引擎注入 kernel globals（用户可直接使用 `pg` 变量）
- on_finish hook：主动广播数据源连接，触发侧边栏展示
"""

from __future__ import annotations

import builtins

from marimo import _loggers
from marimo._runtime.runner.hook_context import (
    OnFinishHookContext,
    PreparationHookContext,
)

LOGGER = _loggers.marimo_logger()


def _inject_tier0_engines(ctx: PreparationHookContext) -> None:
    """preparation hook: 将 sitecustomize.py 中预创建的 SQLAlchemy engine 注入 kernel globals。"""
    engine = getattr(builtins, "_tier0_pg_engine", None)
    if engine is not None and "pg" not in ctx.glbls:
        ctx.glbls["pg"] = engine


def _broadcast_tier0_datasource(ctx: OnFinishHookContext) -> None:
    """on_finish hook: 广播 Tier0 注入的 PG 引擎为数据源，触发侧边栏展示。

    post-execution hook 只扫描 cell.defs，不会发现通过 globals 注入的 pg 变量。
    此钩子在所有 cell 执行完毕后运行一次，主动广播 PG 数据源连接。
    """
    if "pg" not in ctx.glbls:
        return

    # 避免重复广播（同一个 runner 生命周期内只广播一次）
    if ctx.glbls.get("_tier0_pg_broadcasted"):
        return

    try:
        from marimo._messaging.notification import DataSourceConnectionsNotification
        from marimo._messaging.notification_utils import broadcast_notification
        from marimo._sql.get_engines import (
            engine_to_data_source_connection,
            get_engines_from_variables,
        )
        from marimo._types.ids import VariableName

        engines = get_engines_from_variables(
            [(VariableName("pg"), ctx.glbls["pg"])]
        )
        if not engines:
            return

        broadcast_notification(
            DataSourceConnectionsNotification(
                connections=[
                    engine_to_data_source_connection(variable, engine)
                    for variable, engine in engines
                ]
            )
        )
        ctx.glbls["_tier0_pg_broadcasted"] = True
    except Exception as e:
        LOGGER.warning("[Tier0] Failed to broadcast PG datasource: %s", e)
