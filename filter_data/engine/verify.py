"""袋对照与过滤决策溯源：所有过滤决策都必须能回到有效的完备构建代。"""
from __future__ import annotations

from collections import Counter


def _freeze(value):
    if isinstance(value, dict):
        return canonical_row(value)
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(v) for v in value)
    return value


def canonical_row(row: dict) -> tuple:
    return tuple(sorted((k, _freeze(v)) for k, v in row.items()))


def bag_counter(rows) -> Counter:
    return Counter(canonical_row(r) for r in rows)


def bags_equal(a, b) -> bool:
    """袋（多重集）相等：行内容与其重数都一致。"""
    return bag_counter(a) == bag_counter(b)


def bag_diff(a, b) -> dict:
    """a 相对 b 的袋差异（缺失/多出的行及重数）。"""
    ca, cb = bag_counter(a), bag_counter(b)
    missing = ca - cb   # 在 a 有、b 没有（或重数不足）
    extra = cb - ca
    return {'missing': missing, 'extra': extra}


def evidence_problems(result) -> list[str]:
    """校验一次运行的过滤决策是否都可回溯到证据完备的构建代。

    - 每次 filter_activated 的证据必须覆盖该代全部编号片段且声明完备；
    - 存在被过滤丢弃的行 ⇒ 必有对应的完备激活记录，且丢弃归因的代已激活。
    """
    problems = []
    activations = result.log.filter('filter_activated')
    for entry in activations:
        indices = sorted(i for i, _ in entry['evidence'])
        if indices != list(range(entry['total'])):
            problems.append(
                f"代{entry['generation']}激活证据不完备: 片段{indices} ≠ 0..{entry['total'] - 1}")
        if not entry.get('complete', False):
            problems.append(f"代{entry['generation']}激活时未声明完备")
    drops = result.scan.drops_by_generation
    if sum(drops.values()) and not activations:
        problems.append('存在被过滤丢弃的行，但没有任何过滤器激活记录')
    activated = {e['generation'] for e in activations}
    for gen in drops:
        if drops[gen] and gen not in activated:
            problems.append(f'丢弃行归因到未激活的构建代 g{gen}')
    return problems
