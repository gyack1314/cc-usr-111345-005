"""查询装配：分析 → 成本判定 → 协调器/传输 → 扫描 → 连接 → 运行结果。"""
from __future__ import annotations

from dataclasses import dataclass, field

from .budget import MemoryBudget
from .coordinator import BuildSideCoordinator, BuildSpec, DecisionLog, FilterSnapshot
from .executor import (HashJoin, JoinStats, LakeTable, ScanOperator, ScanStats,
                       Transport, TransportEvent, VirtualClock)
from .keys import KeySpec
from .plan import (CostEstimate, CostModel, JoinSpec, PushdownDecision,
                   analyze_pushdown)


@dataclass(frozen=True)
class QueryConfig:
    enable_runtime_filter: bool = True
    force_filter: bool = False   # 不安全：绕过下推判定，仅用于演示禁止项为何禁止
    max_wait: float = 0.0        # 探测侧有界等待（虚拟时间单位），0 = 不等待
    block_cost: float = 1.0      # 每块扫描成本（虚拟时间）
    budget_bytes: int = 1 << 22
    est_selectivity: float | None = None  # 估计选择率；给出才做成本判定
    cost_model: CostModel = field(default_factory=CostModel)


@dataclass
class Scenario:
    """一次查询的全部输入：计划、构建侧、事实表、传输脚本与配置。"""

    label: str
    join_spec: JoinSpec
    key_spec: KeySpec
    build_spec: BuildSpec
    build_rows: list
    probe_table: LakeTable
    events: list
    config: QueryConfig = field(default_factory=QueryConfig)


@dataclass
class RunResult:
    label: str
    rows: list
    log: DecisionLog
    scan: ScanStats
    join: JoinStats
    snapshot: FilterSnapshot | None
    decision: PushdownDecision
    cost: CostEstimate | None
    budget: MemoryBudget
    filter_used: bool
    config: QueryConfig


def run_query(scn: Scenario, *, config: QueryConfig | None = None,
              coordinator_factory=BuildSideCoordinator) -> RunResult:
    cfg = config if config is not None else scn.config
    clock = VirtualClock()
    log = DecisionLog()
    budget = MemoryBudget(cfg.budget_bytes)

    decision = analyze_pushdown(scn.join_spec)
    use_filter = cfg.enable_runtime_filter and (decision.allowed or cfg.force_filter)
    if cfg.enable_runtime_filter and not decision.allowed and not cfg.force_filter:
        log.add(0.0, 'planner', 'filter_not_pushed', reason=decision.reason)
    if cfg.force_filter and not decision.allowed:
        log.add(0.0, 'planner', 'filter_forced_unsafe', reason=decision.reason)

    cost_est = None
    if use_filter and cfg.est_selectivity is not None:
        build_keys = len({scn.key_spec.encode_row(r, scn.join_spec.right_keys)
                          for r in scn.build_rows} - {None})
        filter_bytes = (scn.build_spec.bloom_num_bits // 8
                        if scn.build_spec.filter_kind == 'bloom'
                        else 64 + build_keys * 48)
        cost_est = cfg.cost_model.evaluate(
            probe_rows=scn.probe_table.num_rows,
            probe_blocks=len(scn.probe_table.blocks),
            build_keys=build_keys, selectivity=cfg.est_selectivity,
            filter_bytes=filter_bytes, max_wait=cfg.max_wait)
        log.add(0.0, 'planner', 'cost_evaluated', benefit=round(cost_est.benefit, 1),
                cost=round(cost_est.cost, 1), enabled=cost_est.enabled)
        if not cost_est.enabled:
            use_filter = False
            log.add(0.0, 'planner', 'filter_disabled_by_cost', detail=cost_est.detail)

    coordinator = None
    transport = Transport([])
    if use_filter:
        coordinator = coordinator_factory(
            scn.build_spec, scn.key_spec, scn.join_spec.left_keys, budget, log, clock)
        transport = Transport(scn.events)

    scan = ScanOperator(scn.probe_table, coordinator, transport, clock, log,
                        max_wait=cfg.max_wait, block_cost=cfg.block_cost)
    probe_rows = scan.run()
    join = HashJoin(scn.join_spec)
    rows = join.run(probe_rows, scn.build_rows)
    return RunResult(scn.label, rows, log, scan.stats, join.stats,
                     coordinator.current_snapshot() if coordinator else None,
                     decision, cost_est, budget, use_filter, cfg)


def run_pair(scn: Scenario, **kwargs) -> tuple[RunResult, RunResult, bool]:
    """原计划（不过滤）与加过滤计划对照：袋结果必须一致。"""
    from .verify import bags_equal
    baseline_cfg = QueryConfig(enable_runtime_filter=False,
                               max_wait=scn.config.max_wait,
                               block_cost=scn.config.block_cost)
    baseline = run_query(scn, config=baseline_cfg)
    filtered = run_query(scn, **kwargs)
    return baseline, filtered, bags_equal(baseline.rows, filtered.rows)
