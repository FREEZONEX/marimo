# Copyright 2026 Marimo. All rights reserved.
"""Tier0 hooks for exposing the pre-initialized PostgreSQL engine."""

from __future__ import annotations

import builtins

from marimo import _loggers
from marimo._runtime.runner.hook_context import (
    OnFinishHookContext,
    PreparationHookContext,
)

LOGGER = _loggers.marimo_logger()


def _inject_tier0_engines(ctx: PreparationHookContext) -> None:
    engine = getattr(builtins, "_tier0_pg_engine", None)
    if engine is not None and "pg" not in ctx.glbls:
        ctx.glbls["pg"] = engine


def _broadcast_tier0_datasource(ctx: OnFinishHookContext) -> None:
    if "pg" not in ctx.glbls:
        return
    if ctx.glbls.get("_tier0_pg_broadcasted"):
        return

    try:
        from marimo._messaging.notification import (
            DataSourceConnectionsNotification,
        )
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
