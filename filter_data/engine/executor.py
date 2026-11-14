"""可控本地执行器：虚拟时钟 + 脚本化传输，模拟生产消费与乱序到达。

- Transport：按虚拟时间投递构建侧事件（deliver/fail/cancel），
  deliveries.order 是本地调度顺序，文件顺序不代表业务事件顺序；
- ScanOperator：逐块扫描。等待构建侧有界（max_wait），超时退化为不过滤
  继续扫，绝不以无限等待换取少扫描；过滤器在扫描中途激活是安全的——
  已扫块未过滤、后续块过滤，真实连接兜底，结果不变；
- HashJoin：构建侧保留袋（重复键不去重），过滤器绝不替代真实连接。
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .coordinator import BuildSideCoordinator, DecisionLog
from .plan import JoinSpec, JoinType


class VirtualClock:
    def __init__(self):
        self.now = 0.0

    def advance(self, dt: float) -> None:
        self.now += dt

    def advance_to(self, t: float) -> None:
        if t > self.now:
            self.now = t


@dataclass(frozen=True)
class TransportEvent:
    at: float
    action: str                      # 'deliver' | 'fail' | 'cancel'
    fragment: object = None          # deliver 时为 Fragment
    generation: int | None = None    # fail 时为目标代
    reason: str | None = None


class Transport:
    """可控的本地"网络"：按虚拟时间把构建侧事件投递给协调器。"""

    def __init__(self, events):
        self._events = sorted(events, key=lambda e: e.at)
        self._pos = 0

    def has_pending(self) -> bool:
        return self._pos < len(self._events)

    def next_time(self) -> float | None:
        return self._events[self._pos].at if self.has_pending() else None

    def deliver_due(self, now: float, coordinator: BuildSideCoordinator,
                    log: DecisionLog) -> None:
        while self.has_pending() and self._events[self._pos].at <= now:
            event = self._events[self._pos]
            self._pos += 1
            if event.action == 'deliver':
                coordinator.receive(event.fragment)
            elif event.action == 'fail':
                coordinator.fail(event.generation, event.reason or 'failed')
            elif event.action == 'cancel':
                coordinator.cancel(event.reason or 'cancelled')
            else:
                raise ValueError(f'未知传输动作: {event.action}')


@dataclass(frozen=True)
class LakeTable:
    """事实表的块式布局（模拟湖上按天分区下的列存块）。"""

    name: str
    blocks: tuple  # tuple[tuple[dict, ...]]

    @classmethod
    def from_rows(cls, name: str, rows, rows_per_block: int) -> 'LakeTable':
        blocks = [tuple(rows[i:i + rows_per_block])
                  for i in range(0, len(rows), rows_per_block)]
        return cls(name, tuple(blocks) if blocks else (tuple(),))

    @property
    def num_rows(self) -> int:
        return sum(len(b) for b in self.blocks)


@dataclass
class ScanStats:
    blocks_total: int = 0
    blocks_filtered: int = 0       # 在过滤器激活状态下扫描的块
    blocks_unfiltered: int = 0
    blocks_payload_read: int = 0   # 有幸存行 → 读取载荷
    blocks_payload_skipped: int = 0  # 整块被过滤 → 跳过载荷读取
    rows_scanned: int = 0
    rows_passed: int = 0
    rows_dropped: int = 0
    drops_by_generation: dict = field(default_factory=dict)  # 丢弃行归因到构建代
    waited_units: float = 0.0


class ScanOperator:
    """事实表扫描：键列求值过滤器，幸存行才读取载荷并送往连接。"""

    def __init__(self, table: LakeTable, coordinator: BuildSideCoordinator | None,
                 transport: Transport, clock: VirtualClock, log: DecisionLog,
                 max_wait: float = 0.0, block_cost: float = 1.0):
        self.table = table
        self.coordinator = coordinator
        self.transport = transport
        self.clock = clock
        self.log = log
        self.max_wait = max_wait
        self.block_cost = block_cost
        self.stats = ScanStats()

    def run(self) -> list[dict]:
        out: list[dict] = []
        if self.coordinator is not None and self.max_wait > 0:
            self._bounded_wait()
        for block in self.table.blocks:
            if self.coordinator is not None:
                self.transport.deliver_due(self.clock.now, self.coordinator, self.log)
            snapshot = self.coordinator.current_snapshot() if self.coordinator else None
            self.stats.blocks_total += 1
            self.stats.rows_scanned += len(block)
            if snapshot is None:
                survivors = list(block)
                self.stats.blocks_unfiltered += 1
            else:
                self.stats.blocks_filtered += 1
                survivors = [row for row in block if snapshot.view.might_match(row)]
                dropped = len(block) - len(survivors)
                if dropped:
                    gen = snapshot.generation
                    self.stats.drops_by_generation[gen] = \
                        self.stats.drops_by_generation.get(gen, 0) + dropped
            self.stats.rows_passed += len(survivors)
            self.stats.rows_dropped += len(block) - len(survivors)
            if survivors:
                self.stats.blocks_payload_read += 1
            else:
                self.stats.blocks_payload_skipped += 1
            out.extend(survivors)
            self.clock.advance(self.block_cost)
        # 查询收尾：剩余迟到事件照常投递（它们已无法影响扫描，但必须被正确忽略）
        while self.transport.has_pending():
            self.clock.advance_to(self.transport.next_time())
            self.transport.deliver_due(self.clock.now, self.coordinator, self.log)
        return out

    def _bounded_wait(self) -> None:
        """有界等待构建侧：激活即开工；超时退化为不过滤，绝不无限等待。"""
        start = self.clock.now
        deadline = start + self.max_wait
        while (self.coordinator.current_snapshot() is None
               and self.clock.now < deadline
               and self.transport.has_pending()):
            self.clock.advance_to(min(self.transport.next_time(), deadline))
            self.transport.deliver_due(self.clock.now, self.coordinator, self.log)
        self.stats.waited_units = self.clock.now - start
        if self.coordinator.current_snapshot() is None:
            reason = 'deadline' if self.clock.now >= deadline else 'no_pending_events'
            self.log.add(self.clock.now, 'scan', 'wait_expired',
                         waited=self.stats.waited_units, max_wait=self.max_wait,
                         reason=reason)


@dataclass
class JoinStats:
    build_rows: int = 0
    build_keys_distinct: int = 0
    probe_rows: int = 0
    probe_matched: int = 0
    output_rows: int = 0
    max_multiplicity: int = 0  # 单侧键的最大袋乘数（维表重复键保留证据）


class HashJoin:
    """哈希连接：构建侧 key → 行袋（重复键保留乘数），探测侧逐行匹配。"""

    def __init__(self, spec: JoinSpec):
        self.spec = spec
        self.stats = JoinStats()

    def run(self, left_rows, right_rows) -> list[dict]:
        spec = self.spec
        right_columns = spec.right_columns or (tuple(right_rows[0]) if right_rows else ())
        table: dict[bytes, list[dict]] = {}
        for row in right_rows:
            key = spec.key_spec.encode_row(row, spec.right_keys)
            if key is None:
                continue  # null_unequal：该构建行不可能匹配任何探测行
            table.setdefault(key, []).append(row)  # 袋：重复键全部保留
        self.stats.build_rows = len(right_rows)
        self.stats.build_keys_distinct = len(table)
        self.stats.probe_rows = len(left_rows)

        pairs = []  # (left_row, right_row|None)
        for left in left_rows:
            key = spec.key_spec.encode_row(left, spec.left_keys)
            matches = table.get(key) if key is not None else None
            if matches:
                self.stats.probe_matched += 1
                self.stats.max_multiplicity = max(self.stats.max_multiplicity, len(matches))
            if spec.join_type is JoinType.INNER:
                pairs.extend((left, r) for r in matches or ())
            elif spec.join_type is JoinType.LEFT:
                if matches:
                    pairs.extend((left, r) for r in matches)
                else:
                    pairs.append((left, None))
            elif spec.join_type is JoinType.SEMI:
                if matches:
                    pairs.append((left, None))
            elif spec.join_type is JoinType.ANTI:
                if not matches:
                    pairs.append((left, None))
            else:
                raise ValueError(f'未实现的连接类型: {spec.join_type}')

        for pred in spec.post_predicates:
            pairs = [p for p in pairs if pred.eval_pair(p[0], p[1])]

        if spec.join_type in (JoinType.SEMI, JoinType.ANTI):
            out = [dict(left) for left, _ in pairs]
        else:
            out = [_merge(left, right, right_columns) for left, right in pairs]
        self.stats.output_rows = len(out)
        return out


def _merge(left_row: dict, right_row: dict | None, right_columns) -> dict:
    out = dict(left_row)
    for col in right_columns:
        target = col if col not in left_row else f'{col}#r'
        out[target] = right_row.get(col) if right_row is not None else None
    return out
