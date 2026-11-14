"""运行时键过滤执行层测试：编码、过滤器、协调器、下推分析、端到端袋对照。"""
import unittest

from filter_data.engine.budget import MemoryBudget
from filter_data.engine.coordinator import (BuildSideCoordinator, BuildSpec,
                                            DecisionLog, GenState)
from filter_data.engine.executor import VirtualClock
from filter_data.engine.filters import (BloomFilter, ExactSetFilter,
                                        FilterParamsMismatch, KeyFilterView)
from filter_data.engine.fragments import Fragment, FragmentId
from filter_data.engine.harness import QueryConfig, run_pair, run_query
from filter_data.engine.keys import KeySpec, NullMode
from filter_data.engine.legacy_premature import LegacyPrematureCoordinator
from filter_data.engine.packsrc import scenario_from_pack
from filter_data.engine.plan import JoinSpec, JoinType, Predicate, analyze_pushdown
from filter_data.engine.scenarios import (
    bloom_collision_scenario, bloom_params_mismatch_scenario, cancel_scenario,
    composite_key_scenario, conflict_scenario, cost_disabled_scenario,
    cross_generation_scenario, lake_inner_scenario, left_null_reject_scenario,
    null_equal_scenario, over_budget_scenario, timeout_scenario)
from filter_data.engine.verify import bags_equal, evidence_problems


def make_coordinator(build_spec=None, key_spec=None, budget_bytes=1 << 20):
    key_spec = key_spec or KeySpec(1, ((),), NullMode.NULL_UNEQUAL)
    build_spec = build_spec or BuildSpec('b1', 'snap', num_fragments=2,
                                         filter_kind='exact')
    clock = VirtualClock()
    log = DecisionLog()
    budget = MemoryBudget(budget_bytes)
    coord = BuildSideCoordinator(build_spec, key_spec, ('model',), budget, log, clock)
    return coord, clock, log, budget


def make_fragment(build_spec, key_spec, generation, index, values):
    fid = FragmentId(build_spec.build_id, generation, index,
                     build_spec.num_fragments, key_spec.encoding_id,
                     build_spec.snapshot_id)
    keys = [k for k in (key_spec.encode_values(v) for v in values) if k is not None]
    return Fragment.from_keys(fid, keys)


class KeySpecTests(unittest.TestCase):
    def test_composite_encoding_unambiguous(self):
        spec = KeySpec(2, ((), ()), NullMode.NULL_UNEQUAL)
        self.assertNotEqual(spec.encode_values(('ab', 'c')), spec.encode_values(('a', 'bc')))

    def test_normalization_frozen_in_spec(self):
        plain = KeySpec(1, ((),), NullMode.NULL_UNEQUAL)
        norm = KeySpec(1, (('strip', 'lower'),), NullMode.NULL_UNEQUAL)
        self.assertNotEqual(plain.encoding_id, norm.encoding_id)
        self.assertEqual(norm.encode_values(('  M1 ',)), norm.encode_values(('m1',)))
        self.assertNotEqual(plain.encode_values((' M1',)), plain.encode_values(('m1',)))

    def test_null_modes(self):
        unequal = KeySpec(1, ((),), NullMode.NULL_UNEQUAL)
        equal = KeySpec(1, ((),), NullMode.NULL_EQUAL)
        self.assertIsNone(unequal.encode_values((None,)))
        self.assertIsNotNone(equal.encode_values((None,)))
        self.assertNotEqual(unequal.encoding_id, equal.encoding_id)
        composite = KeySpec(2, ((), ()), NullMode.NULL_UNEQUAL)
        self.assertIsNone(composite.encode_values(('a', None)))

    def test_mixed_type_composite(self):
        spec = KeySpec(3, ((), (), ()), NullMode.NULL_UNEQUAL)
        self.assertEqual(spec.encode_values(('m', 7, True)),
                         spec.encode_values(('m', 7, True)))
        self.assertNotEqual(spec.encode_values(('m', 7, True)),
                            spec.encode_values(('m', 8, True)))


class FilterTests(unittest.TestCase):
    def test_bloom_no_false_negatives(self):
        bloom = BloomFilter(1024, 3, 42)
        keys = [f'key{i}'.encode() for i in range(500)]
        for key in keys:
            bloom.add(key)
        for key in keys:
            self.assertTrue(bloom.might_contain(key))

    def test_bloom_merge_requires_same_params(self):
        a = BloomFilter(64, 2, 7)
        b = BloomFilter(128, 2, 7)
        c = BloomFilter(64, 2, 8)
        with self.assertRaises(FilterParamsMismatch):
            a.merge(b)  # 位长不同禁止 OR
        with self.assertRaises(FilterParamsMismatch):
            a.merge(c)  # 种子不同禁止 OR
        d = BloomFilter(64, 2, 7)
        d.add(b'x')
        a.merge(d)
        self.assertTrue(a.might_contain(b'x'))

    def test_exact_merge_requires_same_encoding(self):
        a = ExactSetFilter('enc-1')
        b = ExactSetFilter('enc-2')
        with self.assertRaises(FilterParamsMismatch):
            a.merge(b)

    def test_serialize_roundtrip(self):
        bloom = BloomFilter(256, 3, 9)
        bloom.add(b'k1')
        clone = BloomFilter.deserialize(bloom.serialize())
        self.assertTrue(clone.might_contain(b'k1'))
        exact = ExactSetFilter('enc')
        exact.add(b'k2')
        clone2 = ExactSetFilter.deserialize(exact.serialize(), 'enc')
        self.assertTrue(clone2.might_contain(b'k2'))

    def test_key_filter_view_null_unequal(self):
        spec = KeySpec(1, ((),), NullMode.NULL_UNEQUAL)
        filt = ExactSetFilter(spec.encoding_id)
        filt.add(spec.encode_values(('M1',)))
        view = KeyFilterView(spec, filt, ('model',))
        self.assertTrue(view.might_match({'model': 'M1'}))
        self.assertFalse(view.might_match({'model': 'M9'}))
        self.assertFalse(view.might_match({'model': None}))  # null 必然无匹配


class BudgetTests(unittest.TestCase):
    def test_reserve_release_peak(self):
        budget = MemoryBudget(100)
        self.assertTrue(budget.try_reserve('a', 60))
        self.assertFalse(budget.try_reserve('b', 50))
        self.assertEqual(budget.peak, 60)
        budget.release_all('a')
        self.assertEqual(budget.used, 0)
        self.assertTrue(budget.try_reserve('b', 100))


class CoordinatorTests(unittest.TestCase):
    def test_no_activation_on_partial_set(self):
        """历史事故回归：只收到一部分键时绝不能当全集启用过滤。"""
        coord, clock, log, _ = make_coordinator()
        frag0 = make_fragment(coord.spec, coord.key_spec, 1, 0, [('M1',)])
        self.assertEqual(coord.receive(frag0), 'accepted')
        self.assertIsNone(coord.current_snapshot())  # 部分键集不得发布
        frag1 = make_fragment(coord.spec, coord.key_spec, 1, 1, [('M2',)])
        self.assertEqual(coord.receive(frag1), 'activated')
        snapshot = coord.current_snapshot()
        self.assertIsNotNone(snapshot)
        self.assertTrue(snapshot.complete)
        self.assertEqual(sorted(i for i, _ in snapshot.evidence), [0, 1])

    def test_out_of_order_and_duplicate(self):
        coord, clock, log, _ = make_coordinator()
        frag1 = make_fragment(coord.spec, coord.key_spec, 1, 1, [('M2',)])
        frag0 = make_fragment(coord.spec, coord.key_spec, 1, 0, [('M1',)])
        self.assertEqual(coord.receive(frag1), 'accepted')   # 乱序：1 先到
        self.assertEqual(coord.receive(frag1), 'ignored_duplicate')  # 重复幂等
        self.assertEqual(coord.receive(frag0), 'activated')

    def test_conflicting_duplicate_fails_generation(self):
        coord, clock, log, _ = make_coordinator()
        coord.receive(make_fragment(coord.spec, coord.key_spec, 1, 0, [('M1',)]))
        conflict = make_fragment(coord.spec, coord.key_spec, 1, 0, [('M9',)])
        self.assertEqual(coord.receive(conflict), 'conflict')
        self.assertIs(coord.state, GenState.FAILED)
        self.assertIsNone(coord.current_snapshot())
        late = make_fragment(coord.spec, coord.key_spec, 1, 1, [('M2',)])
        self.assertEqual(coord.receive(late), 'ignored_failed_generation')

    def test_stale_generation_ignored_and_retry_supersedes(self):
        coord, clock, log, _ = make_coordinator()
        coord.receive(make_fragment(coord.spec, coord.key_spec, 1, 0, [('M1',)]))
        coord.fail(1, 'crash')
        self.assertEqual(coord.receive(
            make_fragment(coord.spec, coord.key_spec, 2, 0, [('M1',)])), 'accepted')
        self.assertEqual(coord.receive(
            make_fragment(coord.spec, coord.key_spec, 1, 1, [('M2',)])),
            'ignored_stale_generation')  # 跨代迟到消息
        self.assertEqual(coord.receive(
            make_fragment(coord.spec, coord.key_spec, 2, 1, [('M2',)])), 'activated')
        self.assertEqual(coord.current_snapshot().generation, 2)

    def test_supersede_reclaims_memory(self):
        coord, clock, log, budget = make_coordinator()
        coord.receive(make_fragment(coord.spec, coord.key_spec, 1, 0, [('M1',)]))
        used_before = budget.used
        self.assertGreater(used_before, 0)
        coord.receive(make_fragment(coord.spec, coord.key_spec, 2, 0, [('M1',)]))
        events = [e['event'] for e in log.entries]
        self.assertIn('generation_superseded', events)

    def test_invalid_identity_rejected(self):
        coord, clock, log, _ = make_coordinator()
        wrong_snap = FragmentId('b1', 1, 0, 2, coord.key_spec.encoding_id, 'other')
        frag = Fragment.from_keys(wrong_snap, [coord.key_spec.encode_values(('M1',))])
        self.assertEqual(coord.receive(frag), 'rejected_invalid')
        self.assertIs(coord.state, GenState.FAILED)

    def test_over_budget_degrades(self):
        coord, clock, log, budget = make_coordinator(budget_bytes=20)
        big = make_fragment(coord.spec, coord.key_spec, 1, 0,
                            [(f'M{i}',) for i in range(10)])
        self.assertEqual(coord.receive(big), 'rejected_over_budget')
        self.assertIs(coord.state, GenState.FAILED)
        self.assertEqual(budget.used, 0)  # 回收

    def test_cancel_is_terminal(self):
        coord, clock, log, _ = make_coordinator()
        coord.cancel('abort')
        self.assertEqual(coord.receive(
            make_fragment(coord.spec, coord.key_spec, 1, 0, [('M1',)])),
            'ignored_cancelled')
        self.assertIsNone(coord.current_snapshot())

    def test_fail_after_active_ignored(self):
        """激活基于完备证据：之后的失败消息不能撤销已证明完备的数据。"""
        coord, clock, log, _ = make_coordinator()
        coord.receive(make_fragment(coord.spec, coord.key_spec, 1, 0, [('M1',)]))
        coord.receive(make_fragment(coord.spec, coord.key_spec, 1, 1, [('M2',)]))
        self.assertIsNotNone(coord.current_snapshot())
        coord.fail(1, 'late_failure')
        self.assertIsNotNone(coord.current_snapshot())
        self.assertIn('fail_ignored', [e['event'] for e in log.entries])


class AnalysisTests(unittest.TestCase):
    def spec(self, join_type, predicates=()):
        key_spec = KeySpec(1, ((),), NullMode.NULL_UNEQUAL)
        return JoinSpec(join_type, ('model',), ('model',), key_spec,
                        right_columns=('id', 'model'), post_predicates=predicates)

    def test_inner_and_semi_allowed(self):
        self.assertTrue(analyze_pushdown(self.spec(JoinType.INNER)).allowed)
        self.assertTrue(analyze_pushdown(self.spec(JoinType.SEMI)).allowed)

    def test_anti_forbidden_on_probe(self):
        decision = analyze_pushdown(self.spec(JoinType.ANTI))
        self.assertFalse(decision.allowed)
        self.assertIn('语义倒置', decision.reason)

    def test_left_preserved_forbidden_nonpreserved_allowed(self):
        self.assertFalse(analyze_pushdown(self.spec(JoinType.LEFT), 'left').allowed)
        self.assertTrue(analyze_pushdown(self.spec(JoinType.LEFT), 'right').allowed)

    def test_null_rejecting_predicate_degenerates_to_inner(self):
        spec = self.spec(JoinType.LEFT, (Predicate('right', 'id', 'not_null'),))
        decision = analyze_pushdown(spec, 'left')
        self.assertTrue(decision.allowed)
        self.assertIs(decision.effective_type, JoinType.INNER)

    def test_is_null_predicate_degenerates_to_anti(self):
        spec = self.spec(JoinType.LEFT, (Predicate('right', 'id', 'is_null'),))
        decision = analyze_pushdown(spec, 'left')
        self.assertFalse(decision.allowed)
        self.assertIs(decision.effective_type, JoinType.ANTI)


class PackScenarioTests(unittest.TestCase):
    def test_all_join_types_bag_equal(self):
        for join_type in (JoinType.INNER, JoinType.SEMI, JoinType.ANTI, JoinType.LEFT):
            with self.subTest(join_type=join_type):
                scn = scenario_from_pack('data/normal.json', join_type=join_type)
                baseline, filtered, equal = run_pair(scn)
                self.assertTrue(equal)
                self.assertEqual(evidence_problems(filtered), [])

    def test_hit_key_in_last_fragment_not_dropped_early(self):
        """命中键 M2 只在最后到达的 f1 中：激活前 r2 必须原样通过。"""
        scn = scenario_from_pack('data/normal.json')
        _, filtered, _ = run_pair(scn)
        activations = filtered.log.filter('filter_activated')
        self.assertEqual(len(activations), 1)
        accepted = filtered.log.filter('fragment_accepted')
        self.assertEqual(len(accepted), 2)  # 收齐才激活
        self.assertEqual(filtered.scan.rows_dropped, 1)  # 只有 null 键 r3 被滤掉
        self.assertEqual(len(filtered.rows), 3)  # r1×d1, r2×d2, r2×d3

    def test_bag_multiplier_preserved(self):
        scn = scenario_from_pack('data/normal.json')
        _, filtered, _ = run_pair(scn)
        self.assertEqual(filtered.join.max_multiplicity, 2)  # d2/d3 重复键
        m2_rows = [r for r in filtered.rows if r['id'] == 'r2']
        self.assertEqual(len(m2_rows), 2)

    def test_legacy_premature_activation_loses_rows(self):
        """复现历史缺陷：首个片段即启用 → r2 被误删；完备性校验能抓住它。"""
        scn = scenario_from_pack('data/normal.json')
        baseline, _, _ = run_pair(scn)
        legacy = run_query(scn, coordinator_factory=LegacyPrematureCoordinator)
        self.assertFalse(bags_equal(baseline.rows, legacy.rows))
        self.assertTrue(evidence_problems(legacy))

    def test_boundary_pack_matches_normal(self):
        _, normal, _ = run_pair(scenario_from_pack('data/normal.json'))
        _, boundary, _ = run_pair(scenario_from_pack('data/boundary.json'))
        self.assertTrue(bags_equal(normal.rows, boundary.rows))
        self.assertEqual([e['event'] for e in normal.log.entries],
                         [e['event'] for e in boundary.log.entries])


class SyntheticScenarioTests(unittest.TestCase):
    def assert_pair_clean(self, scn):
        baseline, filtered, equal = run_pair(scn)
        self.assertTrue(equal, f'{scn.label} 袋结果不一致')
        self.assertEqual(evidence_problems(filtered), [], scn.label)
        return baseline, filtered

    def test_lake_inner_reduces_blocks_and_rows(self):
        baseline, filtered = self.assert_pair_clean(lake_inner_scenario())
        self.assertGreater(filtered.scan.blocks_payload_skipped, 0)
        self.assertLess(filtered.scan.rows_passed, baseline.scan.rows_passed)
        self.assertEqual(filtered.join.max_multiplicity, 2)

    def test_cross_generation_late_message_ignored(self):
        _, filtered = self.assert_pair_clean(cross_generation_scenario())
        stale = [e for e in filtered.log.entries
                 if e['event'] == 'fragment_ignored'
                 and e.get('reason') == 'stale_generation']
        self.assertTrue(stale)
        activation = filtered.log.filter('filter_activated')[0]
        self.assertEqual(activation['generation'], 2)  # 生效的是重试的完备代
        self.assertIn('fragment_duplicate_ignored',
                      [e['event'] for e in filtered.log.entries])

    def test_conflict_degrades(self):
        _, filtered = self.assert_pair_clean(conflict_scenario())
        failed = filtered.log.filter('generation_failed')
        self.assertTrue(any(e['reason'] == 'conflict' for e in failed))
        self.assertIsNone(filtered.snapshot)

    def test_bloom_collision_end_to_end(self):
        scn = bloom_collision_scenario()
        _, filtered = self.assert_pair_clean(scn)
        self.assertGreater(filtered.scan.rows_passed, len(filtered.rows))
        self.assertTrue(all(not r['id'].startswith('fp') for r in filtered.rows))

    def test_bloom_params_mismatch_degrades(self):
        _, filtered = self.assert_pair_clean(bloom_params_mismatch_scenario())
        failed = filtered.log.filter('generation_failed')
        self.assertTrue(any(e['reason'].startswith('params_mismatch') for e in failed))

    def test_null_modes(self):
        _, filtered_eq = self.assert_pair_clean(null_equal_scenario(NullMode.NULL_EQUAL))
        self.assertTrue(any(r['model'] is None for r in filtered_eq.rows))
        _, filtered_un = self.assert_pair_clean(null_equal_scenario(NullMode.NULL_UNEQUAL))
        self.assertTrue(all(r['model'] is not None for r in filtered_un.rows))

    def test_composite_key_normalization(self):
        _, filtered = self.assert_pair_clean(composite_key_scenario())
        self.assertEqual([r['id'] for r in filtered.rows], ['r1'])

    def test_over_budget_degrades_and_reclaims(self):
        _, filtered = self.assert_pair_clean(over_budget_scenario())
        self.assertIsNone(filtered.snapshot)
        self.assertEqual(filtered.budget.used, 0)
        failed = filtered.log.filter('generation_failed')
        self.assertTrue(any(e['reason'] == 'over_budget' for e in failed))

    def test_timeout_bounded_wait(self):
        _, filtered = self.assert_pair_clean(timeout_scenario())
        self.assertEqual(filtered.scan.blocks_filtered, 0)
        expired = filtered.log.filter('wait_expired')
        self.assertTrue(expired and expired[0]['reason'] == 'deadline')

    def test_cancel_degrades(self):
        _, filtered = self.assert_pair_clean(cancel_scenario())
        self.assertIn('build_cancelled', [e['event'] for e in filtered.log.entries])
        self.assertIsNone(filtered.snapshot)

    def test_cost_disables_filter(self):
        _, filtered = self.assert_pair_clean(cost_disabled_scenario())
        self.assertFalse(filtered.filter_used)
        self.assertIn('filter_disabled_by_cost',
                      [e['event'] for e in filtered.log.entries])

    def test_left_null_reject_allows_filter(self):
        _, filtered = self.assert_pair_clean(left_null_reject_scenario())
        self.assertTrue(filtered.decision.allowed)
        self.assertTrue(filtered.filter_used)


class ForcedCounterexampleTests(unittest.TestCase):
    """反例：被禁止的方向若强行过滤，袋结果确实被破坏（证明禁止的必要性）。"""

    def forced(self, join_type):
        scn = scenario_from_pack('data/normal.json', join_type=join_type)
        cfg = QueryConfig(force_filter=True, max_wait=scn.config.max_wait,
                          block_cost=scn.config.block_cost)
        baseline, _, _ = run_pair(scn)
        return baseline, run_query(scn, config=cfg)

    def test_forced_left_loses_preserved_rows(self):
        baseline, forced = self.forced(JoinType.LEFT)
        self.assertFalse(bags_equal(baseline.rows, forced.rows))

    def test_forced_anti_loses_result_rows(self):
        baseline, forced = self.forced(JoinType.ANTI)
        self.assertFalse(bags_equal(baseline.rows, forced.rows))


if __name__ == '__main__':
    unittest.main()
