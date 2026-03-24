# Copyright 2026 Marimo. All rights reserved.
from __future__ import annotations

from typing import Callable

from marimo._runtime import dataflow
from marimo._runtime.runner import cell_runner

# 引入Tier0引擎注入逻辑，作为preparation hook在cell运行前执行，用于初始化/注入自定义存储或计算引擎
from marimo._runtime.runner.hooks_setup_engines import _inject_tier0_engines

# 引入链路追踪工具，用于对关键函数（如_update_stale_statuses）进行OpenTelemetry监控打点
from marimo._tracer import kernel_tracer

PreparationHookType = Callable[[cell_runner.Runner], None]


@kernel_tracer.start_as_current_span("update_stale_statuses")
def _update_stale_statuses(runner: cell_runner.Runner) -> None:
    graph = runner.graph

    if runner.execution_mode == "lazy":
        for cid in dataflow.transitive_closure(
            graph,
            set(runner.cells_to_run),
            inclusive=False,
            relatives=dataflow.get_import_block_relatives(graph),
        ):
            graph.cells[cid].set_stale(stale=True)

    for cid in runner.cells_to_run:
        if graph.is_disabled(cid):
            graph.cells[cid].set_stale(stale=True)
        else:
            graph.cells[cid].set_runtime_state(status="queued")
            if graph.cells[cid].stale:
                graph.cells[cid].set_stale(stale=False)


# 预处理钩子列表，包含状态更新和Tier0引擎注入
PREPARATION_HOOKS: list[PreparationHookType] = [
    _update_stale_statuses,
    _inject_tier0_engines,
]
