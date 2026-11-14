"""运行时键过滤演示：袋结果对照、每阶段统计、退化与决策溯源。

    python -m filter_data.engine.demo
"""
from __future__ import annotations

import sys

from .harness import run_pair, run_query
from .legacy_premature import LegacyPrematureCoordinator
from .packsrc import scenario_from_pack
from .plan import JoinType
from .scenarios import (bloom_collision_scenario, bloom_params_mismatch_scenario,
                        cancel_scenario, composite_key_scenario, conflict_scenario,
                        cost_disabled_scenario, cross_generation_scenario,
                        lake_inner_scenario, left_null_reject_scenario,
                        null_equal_scenario, over_budget_scenario, timeout_scenario)
from .verify import bag_diff, bags_equal, evidence_problems

FAILURES: list[str] = []


def check(ok: bool, message: str) -> None:
    print(f"  {'✓' if ok else '✗'} {message}")
    if not ok:
        FAILURES.append(message)


def _fmt_value(value):
    if isinstance(value, list) and value and isinstance(value[0], list):
        return '[' + ', '.join(f'{i}:{str(c)[:8]}' for i, c in value) + ']'
    text = str(value)
    return text if len(text) <= 96 else text[:93] + '...'


def show_log(result, only=None) -> None:
    for entry in result.log.entries:
        if only and entry['event'] not in only:
            continue
        extras = {k: v for k, v in entry.items() if k not in ('t', 'component', 'event')}
        suffix = '  ' + ' '.join(f'{k}={_fmt_value(v)}' for k, v in extras.items()) if extras else ''
        print(f"    t={entry['t']:>5} [{entry['component']:<10}] {entry['event']}{suffix}")


def show_stages(result) -> None:
    s, j = result.scan, result.join
    snap = result.snapshot
    print(f"    构建阶段: 过滤器={'已激活' if snap else '未启用/未激活'}"
          + (f"(代g{snap.generation}, 键数={snap.key_count}, {snap.memory_bytes}B)" if snap else '')
          + f", 预算峰值={result.budget.peak}B")
    print(f"    扫描阶段: 块 {s.blocks_total}（过滤态 {s.blocks_filtered} / 非过滤态 {s.blocks_unfiltered}），"
          f"载荷读 {s.blocks_payload_read} 块 / 跳过 {s.blocks_payload_skipped} 块，"
          f"行 {s.rows_scanned} → 通过 {s.rows_passed} / 滤掉 {s.rows_dropped}"
          + (f"，等待 {s.waited_units:g}" if s.waited_units else ''))
    print(f"    连接阶段: 构建行 {j.build_rows}（去重键 {j.build_keys_distinct}），"
          f"探测行 {j.probe_rows}，命中 {j.probe_matched}，"
          f"最大袋乘数 {j.max_multiplicity}，输出行 {j.output_rows}")


def compare_pair(label: str, scn, **kwargs):
    baseline, filtered, equal = run_pair(scn, **kwargs)
    problems = evidence_problems(filtered)
    print(f"  [{label}] 原计划 {len(baseline.rows)} 行 vs 加过滤 {len(filtered.rows)} 行")
    check(equal, '袋结果一致（过滤只减少开销，不改变连接结果）')
    check(not problems, '过滤决策可回溯到证据完备的构建代')
    for problem in problems:
        print(f'    ! {problem}')
    return baseline, filtered, equal and not problems


def sec_pack() -> None:
    print('\n== 1. 登记资料包：命中键只在最后一个片段（normal / boundary） ==')
    scn = scenario_from_pack('data/normal.json')
    baseline, filtered, _ = compare_pair('normal 内连接', scn)
    show_stages(filtered)
    print('    关键时间线（f0 到达不激活，f1 到达才激活；r2 在激活前扫描未被误删）:')
    show_log(filtered, only={'fragment_accepted', 'filter_activated'})
    multi = filtered.join.max_multiplicity
    check(multi == 2, f'维表重复键袋乘数保留（M2 × {multi}，过滤器是集合但连接保留袋）')

    scn_b = scenario_from_pack('data/boundary.json')
    _, filtered_b, _ = run_pair(scn_b)
    same_rows = bags_equal(filtered.rows, filtered_b.rows)
    same_events = [e['event'] for e in filtered.log.entries] == \
                  [e['event'] for e in filtered_b.log.entries]
    check(same_rows and same_events,
          'boundary 与 normal 行为一致（deliveries.order 是调度顺序，文件顺序不影响）')

    print('  -- 历史缺陷对照（legacy：首个片段到达即启用过滤）--')
    legacy = run_query(scn, coordinator_factory=LegacyPrematureCoordinator)
    diff = bag_diff(baseline.rows, legacy.rows)
    check(not bags_equal(baseline.rows, legacy.rows),
          '缺陷版本结果确实错误（复现事故：真实请求被筛没）')
    print(f"    缺陷版本丢失的行: {sorted(diff['missing'].keys())}")
    check(bool(evidence_problems(legacy)),
          '缺陷版本无法通过完备性校验（激活时证据不齐）')


def sec_lake() -> None:
    print('\n== 2. 湖表规模：实际减少的读取块与传输行 ==')
    scn = lake_inner_scenario()
    baseline, filtered, _ = compare_pair('湖表内连接', scn)
    show_stages(filtered)
    b, f = baseline.scan, filtered.scan
    print(f"    收益: 载荷块 {b.blocks_payload_read} → {f.blocks_payload_read}"
          f"（少读 {b.blocks_payload_read - f.blocks_payload_read}），"
          f"传输行 {b.rows_passed} → {f.rows_passed}"
          f"（少传 {b.rows_passed - f.rows_passed}）")
    check(f.blocks_payload_skipped > 0, '整块被过滤 → 跳过载荷读取')
    check(f.rows_passed < b.rows_passed, '传输行显著减少')
    check(filtered.join.max_multiplicity == 2, '袋乘数保留（M07 维表重复 × 2）')
    show_log(filtered, only={'fragment_accepted', 'filter_activated', 'cost_evaluated'})


def sec_join_matrix() -> None:
    print('\n== 3. 连接类型矩阵：下推判定依据与端到端对照 ==')
    for join_type in (JoinType.INNER, JoinType.SEMI, JoinType.ANTI, JoinType.LEFT):
        scn = scenario_from_pack('data/normal.json', join_type=join_type,
                                 label=f'pack {join_type.value}')
        baseline, filtered, _ = compare_pair(f'{join_type.value} 连接', scn)
        print(f'    判定: {"允许过滤" if filtered.decision.allowed else "禁止过滤"}'
              f' —— {filtered.decision.reason}')
        if not filtered.decision.allowed:
            check(not filtered.filter_used, '被禁止的方向确实没有启用过滤')
            forced = run_query(scn, config=type(scn.config)(
                force_filter=True, max_wait=scn.config.max_wait,
                block_cost=scn.config.block_cost))
            check(not bags_equal(baseline.rows, forced.rows),
                  f'反例：若强行过滤{join_type.value}探测侧，袋结果确实被破坏'
                  f'（{len(baseline.rows)} → {len(forced.rows)} 行）')
    scn = left_null_reject_scenario()
    _, filtered, _ = compare_pair('左连接+右表null拒绝谓词', scn)
    print(f'    判定: 允许 —— {filtered.decision.reason}')


def sec_degradation() -> None:
    print('\n== 4. 异常与退化：只退化为不过滤，绝不输出错误排除后的结果 ==')
    for scn in (cross_generation_scenario(), conflict_scenario(),
                over_budget_scenario(), timeout_scenario(), cancel_scenario(),
                bloom_params_mismatch_scenario()):
        baseline, filtered, _ = compare_pair(scn.label, scn)
        interesting = [e for e in filtered.log.entries
                       if e['event'] not in ('fragment_accepted',)]
        for entry in interesting:
            extras = {k: v for k, v in entry.items() if k not in ('t', 'component', 'event')}
            suffix = '  ' + ' '.join(f'{k}={_fmt_value(v)}' for k, v in extras.items()) if extras else ''
            print(f"    t={entry['t']:>5} {entry['event']}{suffix}")
        if scn.label.startswith('在途片段超预算'):
            check(filtered.budget.used == 0, '失败后内存已回收（预算占用归零）')
        if scn.label.startswith('构建超时'):
            check(filtered.scan.blocks_filtered == 0, '超时后未过滤任何块（有界等待）')


def sec_filter_semantics() -> None:
    print('\n== 5. 过滤器语义：Bloom 碰撞 / null 键 / 复合键 ==')
    scn = bloom_collision_scenario()
    baseline, filtered, _ = compare_pair(scn.label, scn)
    fp = getattr(scn, 'false_positive_models', [])
    print(f'    假阳性候选 {fp}: 通过过滤器（rows_passed={filtered.scan.rows_passed}），'
          f'真实连接后输出 {len(filtered.rows)} 行')
    check(filtered.scan.rows_passed > len(filtered.rows),
          '假阳性行通过了过滤器（过滤器允许假阳性）')
    check(all(not r['id'].startswith('fp') for r in filtered.rows),
          '假阳性行被真实连接丢弃（结果仍正确）')

    from .keys import NullMode
    scn = null_equal_scenario(NullMode.NULL_EQUAL)
    _, filtered_eq, _ = compare_pair('null_equal', scn)
    null_matched = any(r['model'] is None for r in filtered_eq.rows)
    check(null_matched, 'null_equal 模式下 null 键进过滤器并匹配 null 维键')
    scn_un = null_equal_scenario(NullMode.NULL_UNEQUAL)
    _, filtered_un, _ = compare_pair('null_unequal对照', scn_un)
    check(all(r['model'] is not None for r in filtered_un.rows),
          'null_unequal 模式下 null 键必然不匹配（SQL 默认语义）')

    scn = composite_key_scenario()
    _, filtered, _ = compare_pair(scn.label, scn)
    check(len(filtered.rows) == 1 and filtered.rows[0]['id'] == 'r1',
          "复合键规范化后 ' M1 '/'M1'、大小写差异同键；null 分量行不匹配")


def sec_cost() -> None:
    print('\n== 6. 成本大于收益时禁用 ==')
    scn = cost_disabled_scenario()
    baseline, filtered, _ = compare_pair(scn.label, scn)
    check(not filtered.filter_used, '规划期判定成本>收益，未启用过滤')
    show_log(filtered, only={'cost_evaluated', 'filter_disabled_by_cost'})


def sec_traceability() -> None:
    print('\n== 7. 过滤决策溯源汇总 ==')
    scenarios = [scenario_from_pack('data/normal.json'), lake_inner_scenario(),
                 cross_generation_scenario(), bloom_collision_scenario(),
                 null_equal_scenario(), composite_key_scenario(),
                 left_null_reject_scenario()]
    for scn in scenarios:
        _, filtered, equal = run_pair(scn)
        problems = evidence_problems(filtered)
        for entry in filtered.log.filter('filter_activated'):
            evidence = ', '.join(f'片段{i}:{str(c)[:8]}' for i, c in entry['evidence'])
            print(f"  [{scn.label}] 激活代 g{entry['generation']}（快照 {entry['snapshot_id']}，"
                  f"编码 {entry['encoding_id']}）证据: {evidence}")
        check(equal and not problems, f'[{scn.label}] 袋一致且决策可回溯')


def main() -> int:
    print('运行时键过滤演示：过滤只减少开销，不改变连接袋结果')
    sec_pack()
    sec_lake()
    sec_join_matrix()
    sec_degradation()
    sec_filter_semantics()
    sec_cost()
    sec_traceability()
    print('\n== 总结 ==')
    if FAILURES:
        print(f'{len(FAILURES)} 项检查未通过:')
        for failure in FAILURES:
            print(f'  ✗ {failure}')
        return 1
    print('全部检查通过：袋结果一致、决策可回溯、退化安全。')
    return 0


if __name__ == '__main__':
    sys.exit(main())
