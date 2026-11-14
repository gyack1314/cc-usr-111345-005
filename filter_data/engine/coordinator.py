"""构建侧协调器：按代收集编号片段，完备证据齐备才发布过滤器。

核心不变式（对应历史事故：片段晚到时提前启用过滤筛掉真实请求）：

  只有收齐某一代的全部编号片段、逐一校验身份（执行代/键编码/快照）、
  合并成功且在内存预算内，才发布 FilterSnapshot。快照一旦发布即不可变、
  不可撤销；探测侧只能读到已发布快照。因此任何被过滤掉的行，都可回溯到
  一个证据完备的构建代；只收到一部分键时，绝不把它当全集去排除事实键。

退化原则：构建取消/失败/超预算/超时 → 退化为不做过滤完成原查询；
已发布的快照基于完备证据，不存在"错误排除后的成功结果"。
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from .budget import MemoryBudget
from .filters import BloomFilter, ExactSetFilter, FilterParamsMismatch, KeyFilterView
from .fragments import Fragment
from .keys import KeySpec


class GenState(Enum):
    COLLECTING = 'collecting'  # 收片段中
    ACTIVE = 'active'          # 已发布完备快照（终态）
    FAILED = 'failed'          # 本代失败（冲突/非法/超预算/构建失败）
    SUPERSEDED = 'superseded'  # 被更新的一代取代（重试）
    CANCELLED = 'cancelled'    # 查询级取消（终态）


@dataclass(frozen=True)
class BuildSpec:
    """随计划冻结的构建侧规格：过滤器表示与片段全集大小。"""

    build_id: str
    snapshot_id: str
    num_fragments: int
    filter_kind: str = 'exact'  # 'exact' | 'bloom'
    bloom_num_bits: int = 4096
    bloom_num_hashes: int = 4
    bloom_seed: int = 0x5EED
    max_exact_keys: int = 100_000


@dataclass(frozen=True)
class FilterSnapshot:
    """已发布的过滤器：不可变，携带完备构建代的全部证据。"""

    build_id: str
    generation: int
    snapshot_id: str
    encoding_id: str
    view: KeyFilterView
    evidence: tuple  # 按片段序号排列的 (index, checksum)
    key_count: int | None
    memory_bytes: int
    complete: bool = True  # 安全路径恒为 True；历史缺陷版本发布部分快照时为 False


class DecisionLog:
    """决策日志：退化策略、切换点与回收全部可观察。"""

    def __init__(self):
        self.entries: list[dict] = []

    def add(self, t: float, component: str, event: str, **fields) -> None:
        self.entries.append({'t': t, 'component': component, 'event': event, **fields})

    def filter(self, event: str) -> list[dict]:
        return [e for e in self.entries if e['event'] == event]


class BuildSideCoordinator:
    """单个构建侧（一次连接）的片段收集与过滤器发布。

    代管理：片段身份绑定执行代；更高代的片段到达视为重试，取代当前
    未完成的一代（回收其在途内存）；更低代的迟到消息一律忽略。
    """

    def __init__(self, spec: BuildSpec, key_spec: KeySpec, probe_columns: tuple[str, ...],
                 budget: MemoryBudget, log: DecisionLog, clock):
        self.spec = spec
        self.key_spec = key_spec
        self.probe_columns = probe_columns
        self.budget = budget
        self.log = log
        self.clock = clock
        self._generation: int | None = None
        self._state: GenState | None = None
        self._received: dict[int, Fragment] = {}
        self._snapshot: FilterSnapshot | None = None
        self._cancelled = False

    # ---- 对外状态 ----

    @property
    def generation(self) -> int | None:
        return self._generation

    @property
    def state(self) -> GenState | None:
        return self._state

    def current_snapshot(self) -> FilterSnapshot | None:
        """探测侧唯一读取入口：只有完备证据的快照才会出现在这里。"""
        return self._snapshot

    def _owner(self, generation: int) -> str:
        return f'{self.spec.build_id}#g{generation}'

    # ---- 片段到达 ----

    def receive(self, frag: Fragment) -> str:
        fid = frag.fid
        now = self.clock.now
        if self._cancelled:
            self.log.add(now, self.spec.build_id, 'fragment_ignored',
                         reason='cancelled', generation=fid.generation, index=fid.index)
            return 'ignored_cancelled'
        if self._generation is not None and fid.generation < self._generation:
            self.log.add(now, self.spec.build_id, 'fragment_ignored',
                         reason='stale_generation', generation=fid.generation,
                         index=fid.index, current=self._generation)
            return 'ignored_stale_generation'
        if self._snapshot is not None:
            self.log.add(now, self.spec.build_id, 'fragment_ignored',
                         reason='already_active', generation=fid.generation, index=fid.index)
            return 'ignored_already_active'
        if self._generation is not None and fid.generation == self._generation \
                and self._state is GenState.FAILED:
            self.log.add(now, self.spec.build_id, 'fragment_ignored',
                         reason='generation_already_failed', generation=fid.generation,
                         index=fid.index)
            return 'ignored_failed_generation'
        if self._generation is None or fid.generation > self._generation:
            self._start_generation(fid.generation)

        problem = self._validate_identity(frag)
        if problem is not None:
            self._fail_generation(f'invalid_fragment:{problem}', index=fid.index)
            return 'rejected_invalid'
        if fid.index in self._received:
            if self._received[fid.index].checksum == frag.checksum:
                self.log.add(now, self.spec.build_id, 'fragment_duplicate_ignored',
                             generation=fid.generation, index=fid.index)
                return 'ignored_duplicate'
            self._fail_generation('conflict', index=fid.index)
            return 'conflict'
        owner = self._owner(fid.generation)
        if not self.budget.try_reserve(owner, frag.approx_bytes):
            self._fail_generation('over_budget', index=fid.index,
                                  approx_bytes=frag.approx_bytes,
                                  budget_available=self.budget.available)
            return 'rejected_over_budget'
        self._received[fid.index] = frag
        self.log.add(now, self.spec.build_id, 'fragment_accepted',
                     generation=fid.generation, index=fid.index, total=fid.total,
                     checksum=frag.checksum, approx_bytes=frag.approx_bytes,
                     received=len(self._received))
        if len(self._received) == self.spec.num_fragments:
            self._activate()
            return 'activated'
        return 'accepted'

    def _validate_identity(self, frag: Fragment) -> str | None:
        """片段身份必须绑定本计划的执行代、键编码与快照。"""
        fid = frag.fid
        if fid.build_id != self.spec.build_id:
            return f'build_id {fid.build_id!r} != {self.spec.build_id!r}'
        if fid.snapshot_id != self.spec.snapshot_id:
            return f'snapshot {fid.snapshot_id!r} != {self.spec.snapshot_id!r}'
        if fid.encoding_id != self.key_spec.encoding_id:
            return f'encoding {fid.encoding_id!r} != {self.key_spec.encoding_id!r}'
        if fid.total != self.spec.num_fragments:
            return f'total {fid.total} != {self.spec.num_fragments}'
        if not (0 <= fid.index < fid.total):
            return f'index {fid.index} 越界'
        if frag.kind != self.spec.filter_kind:
            return f'kind {frag.kind!r} != {self.spec.filter_kind!r}'
        return None

    # ---- 代生命周期 ----

    def _start_generation(self, generation: int) -> None:
        if self._generation is not None and self._state is GenState.COLLECTING:
            missing = sorted(set(range(self.spec.num_fragments)) - set(self._received))
            freed = self.budget.release_all(self._owner(self._generation))
            self.log.add(self.clock.now, self.spec.build_id, 'generation_superseded',
                         generation=self._generation, by=generation,
                         missing=missing, released_bytes=freed)
        self._generation = generation
        self._state = GenState.COLLECTING
        self._received = {}
        self.log.add(self.clock.now, self.spec.build_id, 'generation_started',
                     generation=generation, total=self.spec.num_fragments)

    def _fail_generation(self, reason: str, **fields) -> None:
        freed = self.budget.release_all(self._owner(self._generation))
        missing = sorted(set(range(self.spec.num_fragments)) - set(self._received))
        self._state = GenState.FAILED
        self._received = {}
        self.log.add(self.clock.now, self.spec.build_id, 'generation_failed',
                     generation=self._generation, reason=reason,
                     missing=missing, released_bytes=freed, **fields)

    def fail(self, generation: int, reason: str) -> None:
        """构建侧主动报告失败：退化为不做过滤，绝不撤销已发布的完备快照。"""
        now = self.clock.now
        if self._cancelled or self._snapshot is not None:
            self.log.add(now, self.spec.build_id, 'fail_ignored',
                         generation=generation, reason=reason,
                         because='terminal' if self._cancelled else 'already_active')
            return
        if self._generation is None or generation != self._generation \
                or self._state is not GenState.COLLECTING:
            self.log.add(now, self.spec.build_id, 'fail_ignored',
                         generation=generation, reason=reason, because='stale_or_future')
            return
        self._fail_generation(f'build_failed:{reason}')

    def cancel(self, reason: str) -> None:
        """查询级取消：终态退化，之后到达的片段一律忽略。"""
        freed = 0
        if self._generation is not None:
            freed = self.budget.release_all(self._owner(self._generation))
        self._cancelled = True
        self._state = GenState.CANCELLED
        self._received = {}
        self.log.add(self.clock.now, self.spec.build_id, 'build_cancelled',
                     reason=reason, released_bytes=freed)

    # ---- 完备性闸门：唯一的过滤器发布点 ----

    def _merge_received(self):
        """合并本代已收片段。只在证据齐备后由 _activate 调用（安全路径）。"""
        if self.spec.filter_kind == ExactSetFilter.KIND:
            merged = ExactSetFilter(self.key_spec.encoding_id)
            for index in sorted(self._received):
                merged.merge(self._received[index].decode_exact())
            return merged, len(merged)
        merged = BloomFilter(self.spec.bloom_num_bits, self.spec.bloom_num_hashes,
                             self.spec.bloom_seed)
        for index in sorted(self._received):
            merged.merge(self._received[index].decode_bloom())  # 参数不符即抛错
        return merged, None

    def _activate(self) -> None:
        """收齐全部编号片段 → 合并 → 预算内 → 原子发布。证据随快照冻结。"""
        generation = self._generation
        try:
            merged, key_count = self._merge_received()
        except FilterParamsMismatch as exc:
            self._fail_generation(f'params_mismatch:{exc}')
            return
        if key_count is not None and key_count > self.spec.max_exact_keys:
            self._fail_generation('too_many_keys', key_count=key_count)
            return
        owner = self._owner(generation)
        self.budget.release_all(owner)  # 释放在途片段账目，改记合并后快照
        if not self.budget.try_reserve(owner, merged.memory_bytes):
            self._fail_generation('over_budget', merged_bytes=merged.memory_bytes,
                                  budget_available=self.budget.available)
            return
        evidence = tuple((i, self._received[i].checksum)
                         for i in sorted(self._received))
        self._snapshot = FilterSnapshot(
            build_id=self.spec.build_id,
            generation=generation,
            snapshot_id=self.spec.snapshot_id,
            encoding_id=self.key_spec.encoding_id,
            view=KeyFilterView(self.key_spec, merged, self.probe_columns),
            evidence=evidence,
            key_count=key_count,
            memory_bytes=merged.memory_bytes,
            complete=True,
        )
        self._state = GenState.ACTIVE
        self._received = {}
        self.log.add(self.clock.now, self.spec.build_id, 'filter_activated',
                     generation=generation, snapshot_id=self.spec.snapshot_id,
                     encoding_id=self.key_spec.encoding_id,
                     total=self.spec.num_fragments,
                     evidence=[list(item) for item in evidence],
                     key_count=key_count, memory_bytes=merged.memory_bytes,
                     complete=True)
