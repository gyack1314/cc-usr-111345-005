"""计划要素与下推合法性分析：能否过滤取决于连接位置、保留关系与谓词语义。

判定矩阵（探测侧 = 被过滤的一侧，构建侧 = 产生候选键的一侧）：

- INNER      ：允许。未匹配的探测行本就不在结果中；过滤器无假阴性，
               键编码与 null 模式随计划冻结，与连接判定完全一致。
- SEMI       ：允许。半连接只输出有匹配的探测行，被过滤行必然无匹配。
- ANTI       ：禁止（对探测侧）。反连接输出的恰是无匹配行，按键集合
               排除会删掉结果本身（语义倒置）。只允许反向过滤构建侧。
- LEFT/RIGHT ：探测侧为保留侧 → 禁止。未匹配的保留侧行以 null 扩展
               出现在结果中，过滤会改变袋结果。
               探测侧为非保留侧 → 允许。非保留侧未匹配行本就不出现。
- FULL       ：两侧皆保留，两个方向都禁止（本执行层不实现 FULL）。
- 谓词语义   ：左连接 + 右表 null 拒绝谓词（IS NOT NULL / 等值）→
               null 扩展行必被谓词删除，连接退化为内连接语义 → 允许；
               左连接 + 右表 IS NULL 谓词 → 退化为反连接语义 → 禁止。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from .keys import KeySpec


class JoinType(Enum):
    INNER = 'inner'
    LEFT = 'left'    # 左表为保留侧
    SEMI = 'semi'    # 左半连接：输出有匹配的左行（各一次）
    ANTI = 'anti'    # 左反连接：输出无匹配的左行


@dataclass(frozen=True)
class Predicate:
    """连接后谓词。side 指明作用于连接的哪一侧（合并前求值，无列名歧义）。"""

    side: str      # 'left' | 'right'
    column: str
    op: str        # 'is_null' | 'not_null' | 'eq'
    value: object = None

    @property
    def null_rejecting(self) -> bool:
        """是否拒绝 null 扩展行（该侧为 null 时谓词必不成立）。"""
        return self.op in ('not_null', 'eq')

    def eval_pair(self, left_row, right_row) -> bool:
        row = left_row if self.side == 'left' else right_row
        value = row.get(self.column) if row is not None else None
        if self.op == 'is_null':
            return value is None
        if self.op == 'not_null':
            return value is not None
        if self.op == 'eq':
            return value is not None and value == self.value
        raise ValueError(f'未知谓词算子: {self.op}')


@dataclass(frozen=True)
class JoinSpec:
    """连接节点：等值键（可为复合键）、键规范、连接后谓词，随计划冻结。"""

    join_type: JoinType
    left_keys: tuple[str, ...]
    right_keys: tuple[str, ...]
    key_spec: KeySpec
    right_columns: tuple[str, ...] = ()       # 右表模式（左连接补 null 用）
    post_predicates: tuple[Predicate, ...] = ()


@dataclass(frozen=True)
class PushdownDecision:
    allowed: bool
    reason: str
    effective_type: JoinType


def _effective_type(spec: JoinSpec) -> tuple[JoinType, str]:
    """连接后谓词可能改变保留关系的语义，先退化再套判定矩阵。"""
    if spec.join_type is JoinType.LEFT:
        for pred in spec.post_predicates:
            if pred.side == 'right' and pred.null_rejecting:
                return JoinType.INNER, (
                    f'连接后谓词 {pred.column} {pred.op} 拒绝右表 null 扩展行，'
                    f'左连接退化为内连接语义')
        for pred in spec.post_predicates:
            if pred.side == 'right' and pred.op == 'is_null':
                return JoinType.ANTI, (
                    f'连接后谓词 {pred.column} IS NULL 只保留 null 扩展行，'
                    f'左连接退化为反连接语义')
    return spec.join_type, '无改变保留关系的连接后谓词'


def analyze_pushdown(spec: JoinSpec, probe_side: str = 'left') -> PushdownDecision:
    """判定是否允许用构建侧候选键过滤探测侧扫描，并给出依据。"""
    effective, why = _effective_type(spec)
    if effective is JoinType.INNER:
        return PushdownDecision(True, (
            '内连接：未匹配的探测行本就不在连接结果中；过滤器无假阴性，'
            '键编码与 null 模式随计划冻结，与连接判定一致。' + why), effective)
    if effective is JoinType.SEMI:
        return PushdownDecision(True, (
            '半连接：只输出有匹配的探测行，被过滤行必然无匹配，本就不在输出中。'
            + why), effective)
    if effective is JoinType.ANTI:
        return PushdownDecision(False, (
            '反连接：输出的恰是无匹配的探测行，按键集合排除会删掉结果本身'
            '（语义倒置）；只允许反向用探测侧键过滤构建侧，本计划方向不符，禁止。'
            + why), effective)
    # LEFT：保留关系取决于探测侧位置
    if probe_side == 'left':
        return PushdownDecision(False, (
            '左连接：探测侧为保留侧，未匹配行以 null 扩展出现在结果中，'
            '过滤保留侧会改变袋结果，禁止。' + why), effective)
    return PushdownDecision(True, (
        '左连接：探测侧为非保留侧，其未匹配行本就不出现在结果中，允许过滤。'
        + why), effective)


@dataclass(frozen=True)
class CostEstimate:
    benefit: float
    cost: float
    enabled: bool
    detail: str


@dataclass(frozen=True)
class CostModel:
    """规划期成本模型：成本大于收益时允许禁用过滤。"""

    row_transfer_cost: float = 1.0     # 每行传输/物化成本
    block_payload_cost: float = 40.0   # 每块载荷读取成本
    build_key_cost: float = 2.0        # 每构建键成本
    filter_byte_cost: float = 0.02     # 过滤器每字节内存成本
    wait_unit_cost: float = 20.0       # 每单位等待时间成本
    margin: float = 1.0                # 收益必须超过成本 × margin

    def evaluate(self, *, probe_rows: int, probe_blocks: int, build_keys: int,
                 selectivity: float, filter_bytes: int, max_wait: float) -> CostEstimate:
        saved_rows = (1.0 - selectivity) * probe_rows
        saved_blocks = (1.0 - selectivity) * probe_blocks
        benefit = saved_rows * self.row_transfer_cost + saved_blocks * self.block_payload_cost
        cost = (build_keys * self.build_key_cost
                + filter_bytes * self.filter_byte_cost
                + max_wait * self.wait_unit_cost)
        enabled = benefit > cost * self.margin
        detail = (f'收益≈{benefit:.0f}（少传 {saved_rows:.0f} 行 / 少读 {saved_blocks:.0f} 块），'
                  f'成本≈{cost:.0f}（构建 {build_keys} 键 + 过滤器 {filter_bytes}B '
                  f'+ 等待 {max_wait}）→ {"启用" if enabled else "禁用"}')
        return CostEstimate(benefit, cost, enabled, detail)
