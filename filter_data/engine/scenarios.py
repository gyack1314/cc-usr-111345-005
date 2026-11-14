"""合成场景：在可控本地执行器上构造生产消费、乱序到达与各类异常。"""
from __future__ import annotations

from .coordinator import BuildSpec
from .executor import LakeTable, TransportEvent
from .filters import BloomFilter
from .fragments import Fragment, FragmentId
from .harness import QueryConfig, Scenario
from .keys import KeySpec, NullMode
from .plan import JoinSpec, JoinType, Predicate


def _fid(build_spec: BuildSpec, key_spec: KeySpec, generation: int, index: int) -> FragmentId:
    return FragmentId(build_id=build_spec.build_id, generation=generation, index=index,
                      total=build_spec.num_fragments, encoding_id=key_spec.encoding_id,
                      snapshot_id=build_spec.snapshot_id)


def _encode_all(key_spec: KeySpec, values) -> list[bytes]:
    return [k for k in (key_spec.encode_values(v) for v in values) if k is not None]


def exact_delivery(at, build_spec, key_spec, generation, index, values) -> TransportEvent:
    frag = Fragment.from_keys(_fid(build_spec, key_spec, generation, index),
                              _encode_all(key_spec, values))
    return TransportEvent(at=float(at), action='deliver', fragment=frag)


def bloom_delivery(at, build_spec, key_spec, generation, index, values,
                   num_bits=None) -> TransportEvent:
    bloom = BloomFilter(num_bits or build_spec.bloom_num_bits,
                        build_spec.bloom_num_hashes, build_spec.bloom_seed)
    for key in _encode_all(key_spec, values):
        bloom.add(key)
    frag = Fragment.from_bloom(_fid(build_spec, key_spec, generation, index), bloom)
    return TransportEvent(at=float(at), action='deliver', fragment=frag)


def _single_key_spec(null_mode=NullMode.NULL_UNEQUAL) -> KeySpec:
    return KeySpec(arity=1, normalizations=((),), null_mode=null_mode)


def lake_inner_scenario(blocks=240, rows_per_block=50, versions=20,
                        selected=('M03', 'M07')) -> Scenario:
    """湖表内连接：按 model 聚簇的块布局，少数模型版本被选中。

    - 命中键 M07 的维表记录重复（袋乘数 2），过滤器是集合但连接保留袋；
    - 片段乱序到达（index 1 先于 index 0），收齐才激活；
    - 大多数块整批被过滤 → 跳过载荷读取。
    """
    key_spec = _single_key_spec()
    build_spec = BuildSpec('dim_model', 'lake-v1', num_fragments=2, filter_kind='exact')
    facts = [{'id': f'r{b}_{i}', 'model': f'M{b % versions:02d}',
              'tenant': f'T{i % 5}', 'tokens': (b * 7 + i) % 997}
             for b in range(blocks) for i in range(rows_per_block)]
    dims = [{'id': f'd_M{v:02d}', 'model': f'M{v:02d}'} for v in range(versions)]
    build_rows = [d for d in dims if d['model'] in selected]
    build_rows.append({'id': 'd_M07_dup', 'model': 'M07'})  # 合法维表袋记录
    events = [
        exact_delivery(2, build_spec, key_spec, 1, 1, [('M07',), ('M07',)]),  # 乱序：1 先到
        exact_delivery(5, build_spec, key_spec, 1, 0, [('M03',)]),
    ]
    join_spec = JoinSpec(JoinType.INNER, ('model',), ('model',), key_spec,
                         right_columns=('id', 'model'))
    return Scenario('湖表内连接(聚簇块)', join_spec, key_spec, build_spec, build_rows,
                    LakeTable.from_rows('calls', facts, rows_per_block), events,
                    QueryConfig(max_wait=10.0, block_cost=1.0, est_selectivity=0.1))


def cross_generation_scenario() -> Scenario:
    """跨代迟到消息：g1 构建失败 → g2 重试收齐激活 → g1 迟到片段被忽略。"""
    key_spec = _single_key_spec()
    build_spec = BuildSpec('dim_model', 'snap-x', num_fragments=2, filter_kind='exact')
    facts = [{'id': 'r1', 'model': 'M1'}, {'id': 'r2', 'model': 'M2'},
             {'id': 'r3', 'model': 'M1'}, {'id': 'r4', 'model': 'M9'}]
    dims = [{'id': 'd1', 'model': 'M1'}, {'id': 'd2', 'model': 'M2'}]
    events = [
        exact_delivery(1, build_spec, key_spec, 1, 0, [('M1',)]),
        TransportEvent(at=2.0, action='fail', generation=1, reason='producer_crash'),
        exact_delivery(3, build_spec, key_spec, 2, 0, [('M1',)]),       # g2 重试
        exact_delivery(3.5, build_spec, key_spec, 2, 0, [('M1',)]),     # 重复投递：幂等忽略
        exact_delivery(4, build_spec, key_spec, 2, 1, [('M2',)]),       # g2 收齐 → 激活
        exact_delivery(5, build_spec, key_spec, 1, 1, [('M2',)]),       # g1 迟到：忽略
    ]
    join_spec = JoinSpec(JoinType.INNER, ('model',), ('model',), key_spec,
                         right_columns=('id', 'model'))
    return Scenario('跨代迟到消息', join_spec, key_spec, build_spec, dims,
                    LakeTable.from_rows('facts', facts, 1), events,
                    QueryConfig(max_wait=10.0, block_cost=1.0))


def conflict_scenario() -> Scenario:
    """同代同序号不同内容 → 冲突，本代失败，退化为不过滤。"""
    key_spec = _single_key_spec()
    build_spec = BuildSpec('dim_model', 'snap-c', num_fragments=2, filter_kind='exact')
    facts = [{'id': 'r1', 'model': 'M1'}, {'id': 'r2', 'model': 'M2'}]
    dims = [{'id': 'd1', 'model': 'M1'}, {'id': 'd2', 'model': 'M2'}]
    events = [
        exact_delivery(1, build_spec, key_spec, 1, 0, [('M1',)]),
        exact_delivery(2, build_spec, key_spec, 1, 0, [('M1',), ('M9',)]),  # 内容冲突
        exact_delivery(3, build_spec, key_spec, 1, 1, [('M2',)]),           # 代已失败：忽略
    ]
    join_spec = JoinSpec(JoinType.INNER, ('model',), ('model',), key_spec,
                         right_columns=('id', 'model'))
    return Scenario('片段内容冲突退化', join_spec, key_spec, build_spec, dims,
                    LakeTable.from_rows('facts', facts, 1), events,
                    QueryConfig(max_wait=10.0, block_cost=1.0))


def bloom_collision_scenario() -> Scenario:
    """Bloom 碰撞：假阳性行通过过滤器，真实连接把它丢掉，结果不变。"""
    key_spec = _single_key_spec()
    build_spec = BuildSpec('dim_bloom', 'snap-bloom', num_fragments=2,
                           filter_kind='bloom', bloom_num_bits=64,
                           bloom_num_hashes=2, bloom_seed=7)
    merged = BloomFilter(64, 2, 7)
    for key in _encode_all(key_spec, [('M1',), ('M2',)]):
        merged.add(key)
    false_positives, clean_misses = [], []
    for i in range(4000):
        candidate = f'X{i:04d}'
        (false_positives if merged.might_contain(key_spec.encode_values((candidate,)))
         else clean_misses).append(candidate)
        if len(false_positives) >= 3 and len(clean_misses) >= 5:
            break
    facts = ([{'id': 'r1', 'model': 'M1'}, {'id': 'r2', 'model': 'M2'}]
             + [{'id': f'fp{i}', 'model': c} for i, c in enumerate(false_positives[:3])]
             + [{'id': f'm{i}', 'model': c} for i, c in enumerate(clean_misses[:5])])
    dims = [{'id': 'd1', 'model': 'M1'}, {'id': 'd2', 'model': 'M2'}]
    events = [bloom_delivery(1, build_spec, key_spec, 1, 0, [('M1',)]),
              bloom_delivery(2, build_spec, key_spec, 1, 1, [('M2',)])]
    join_spec = JoinSpec(JoinType.INNER, ('model',), ('model',), key_spec,
                         right_columns=('id', 'model'))
    scn = Scenario('Bloom碰撞(假阳性)', join_spec, key_spec, build_spec, dims,
                   LakeTable.from_rows('facts', facts, 1), events,
                   QueryConfig(max_wait=5.0, block_cost=1.0))
    scn.false_positive_models = false_positives[:3]  # 供演示标注
    return scn


def bloom_params_mismatch_scenario() -> Scenario:
    """不同位长的位图到达同一代：拒绝直接 OR，本代失败退化。"""
    key_spec = _single_key_spec()
    build_spec = BuildSpec('dim_bloom', 'snap-mix', num_fragments=2,
                           filter_kind='bloom', bloom_num_bits=64,
                           bloom_num_hashes=2, bloom_seed=7)
    facts = [{'id': 'r1', 'model': 'M1'}, {'id': 'r2', 'model': 'M2'}]
    dims = [{'id': 'd1', 'model': 'M1'}, {'id': 'd2', 'model': 'M2'}]
    events = [
        bloom_delivery(1, build_spec, key_spec, 1, 0, [('M1',)]),
        bloom_delivery(2, build_spec, key_spec, 1, 1, [('M2',)], num_bits=128),  # 位长不符
    ]
    join_spec = JoinSpec(JoinType.INNER, ('model',), ('model',), key_spec,
                         right_columns=('id', 'model'))
    return Scenario('Bloom参数不一致退化', join_spec, key_spec, build_spec, dims,
                    LakeTable.from_rows('facts', facts, 1), events,
                    QueryConfig(max_wait=5.0, block_cost=1.0))


def null_equal_scenario(null_mode: NullMode = NullMode.NULL_EQUAL) -> Scenario:
    """null 键两模式对照：null_equal 下 null 是普通键值，进过滤器、可匹配；
    null_unequal 下 null 键必然不匹配（不插入过滤器，探测侧直接判否）。"""
    key_spec = _single_key_spec(null_mode)
    build_spec = BuildSpec('dim_model', 'snap-null', num_fragments=2, filter_kind='exact')
    facts = [{'id': 'r1', 'model': 'M1'}, {'id': 'r2', 'model': None},
             {'id': 'r3', 'model': 'M9'}]
    dims = [{'id': 'd1', 'model': 'M1'}, {'id': 'd2', 'model': None}]
    events = [exact_delivery(1, build_spec, key_spec, 1, 0, [('M1',)]),
              exact_delivery(2, build_spec, key_spec, 1, 1, [(None,)])]
    join_spec = JoinSpec(JoinType.INNER, ('model',), ('model',), key_spec,
                         right_columns=('id', 'model'))
    return Scenario(f'null键({null_mode.value})', join_spec, key_spec, build_spec, dims,
                    LakeTable.from_rows('facts', facts, 1), events,
                    QueryConfig(max_wait=5.0, block_cost=1.0))


def composite_key_scenario() -> Scenario:
    """复合键 + 规范化：' M1 ' 与 'M1'、大小写差异经冻结规则后同键。"""
    key_spec = KeySpec(arity=2, normalizations=(('strip', 'lower'), ('strip', 'upper')),
                       null_mode=NullMode.NULL_UNEQUAL)
    build_spec = BuildSpec('dim_model_region', 'snap-comp', num_fragments=1,
                           filter_kind='exact')
    facts = [{'id': 'r1', 'model': 'M1', 'region': 'cn-north'},
             {'id': 'r2', 'model': 'm1', 'region': 'cn-south'},
             {'id': 'r3', 'model': None, 'region': 'cn-north'}]
    dims = [{'id': 'd1', 'model': ' m1 ', 'region': 'CN-NORTH '}]
    events = [exact_delivery(1, build_spec, key_spec, 1, 0,
                             [(' m1 ', 'CN-NORTH ')])]
    join_spec = JoinSpec(JoinType.INNER, ('model', 'region'), ('model', 'region'),
                         key_spec, right_columns=('id', 'model', 'region'))
    return Scenario('复合键规范化', join_spec, key_spec, build_spec, dims,
                    LakeTable.from_rows('facts', facts, 1), events,
                    QueryConfig(max_wait=5.0, block_cost=1.0))


def over_budget_scenario() -> Scenario:
    """在途片段超预算：本代失败并回收内存，退化为不过滤完成查询。"""
    key_spec = _single_key_spec()
    build_spec = BuildSpec('dim_model', 'snap-budget', num_fragments=2,
                           filter_kind='exact')
    facts = [{'id': f'r{i}', 'model': f'M{i}'} for i in range(1, 10)]
    dims = [{'id': f'd{i}', 'model': f'M{i}'} for i in range(1, 10)]
    events = [
        exact_delivery(1, build_spec, key_spec, 1, 0,
                       [(f'M{i}',) for i in range(1, 6)]),   # 约 69B，预算内
        exact_delivery(2, build_spec, key_spec, 1, 1,
                       [(f'M{i}',) for i in range(6, 10)]),  # 累计超 80B → 失败
    ]
    join_spec = JoinSpec(JoinType.INNER, ('model',), ('model',), key_spec,
                         right_columns=('id', 'model'))
    return Scenario('在途片段超预算退化', join_spec, key_spec, build_spec, dims,
                    LakeTable.from_rows('facts', facts, 1), events,
                    QueryConfig(max_wait=10.0, block_cost=1.0, budget_bytes=80))


def timeout_scenario() -> Scenario:
    """有界等待：构建侧超过等待上限 → 不过滤先扫，绝不无限等待。"""
    key_spec = _single_key_spec()
    build_spec = BuildSpec('dim_model', 'snap-slow', num_fragments=2, filter_kind='exact')
    facts = [{'id': 'r1', 'model': 'M1'}, {'id': 'r2', 'model': 'M2'},
             {'id': 'r3', 'model': 'M9'}]
    dims = [{'id': 'd1', 'model': 'M1'}, {'id': 'd2', 'model': 'M2'}]
    events = [exact_delivery(100, build_spec, key_spec, 1, 0, [('M1',)]),
              exact_delivery(101, build_spec, key_spec, 1, 1, [('M2',)])]
    join_spec = JoinSpec(JoinType.INNER, ('model',), ('model',), key_spec,
                         right_columns=('id', 'model'))
    return Scenario('构建超时退化', join_spec, key_spec, build_spec, dims,
                    LakeTable.from_rows('facts', facts, 1), events,
                    QueryConfig(max_wait=5.0, block_cost=1.0))


def cancel_scenario() -> Scenario:
    """构建取消：退化为不过滤完成原查询，之后到达的片段一律忽略。"""
    key_spec = _single_key_spec()
    build_spec = BuildSpec('dim_model', 'snap-cancel', num_fragments=2,
                           filter_kind='exact')
    facts = [{'id': 'r1', 'model': 'M1'}, {'id': 'r2', 'model': 'M2'}]
    dims = [{'id': 'd1', 'model': 'M1'}, {'id': 'd2', 'model': 'M2'}]
    events = [TransportEvent(at=1.0, action='cancel', reason='build_side_aborted'),
              exact_delivery(2, build_spec, key_spec, 1, 0, [('M1',)]),
              exact_delivery(3, build_spec, key_spec, 1, 1, [('M2',)])]
    join_spec = JoinSpec(JoinType.INNER, ('model',), ('model',), key_spec,
                         right_columns=('id', 'model'))
    return Scenario('构建取消退化', join_spec, key_spec, build_spec, dims,
                    LakeTable.from_rows('facts', facts, 1), events,
                    QueryConfig(max_wait=10.0, block_cost=1.0))


def cost_disabled_scenario() -> Scenario:
    """成本大于收益：维表几乎不过滤（选择率 0.95）+ 等待昂贵 → 规划期禁用。"""
    key_spec = _single_key_spec()
    build_spec = BuildSpec('dim_model', 'snap-cost', num_fragments=1, filter_kind='exact')
    versions = 20
    selected = tuple(f'M{v:02d}' for v in range(versions) if v != 11)  # 19/20 被选中
    facts = [{'id': f'r{b}_{i}', 'model': f'M{b % versions:02d}'}
             for b in range(60) for i in range(50)]
    dims = [{'id': f'd{v}', 'model': m} for v, m in enumerate(selected)]
    events = [exact_delivery(1, build_spec, key_spec, 1, 0, [(m,) for m in selected])]
    join_spec = JoinSpec(JoinType.INNER, ('model',), ('model',), key_spec,
                         right_columns=('id', 'model'))
    return Scenario('成本大于收益禁用', join_spec, key_spec, build_spec, dims,
                    LakeTable.from_rows('facts', facts, 50), events,
                    QueryConfig(max_wait=50.0, block_cost=1.0, est_selectivity=0.95))


def left_null_reject_scenario() -> Scenario:
    """左连接 + 右表 null 拒绝谓词 → 退化为内连接语义 → 允许过滤。"""
    key_spec = _single_key_spec()
    build_spec = BuildSpec('dim_model', 'snap-nr', num_fragments=1, filter_kind='exact')
    facts = [{'id': 'r1', 'model': 'M1'}, {'id': 'r2', 'model': 'M9'},
             {'id': 'r3', 'model': None}]
    dims = [{'id': 'd1', 'model': 'M1'}]
    events = [exact_delivery(1, build_spec, key_spec, 1, 0, [('M1',)])]
    join_spec = JoinSpec(JoinType.LEFT, ('model',), ('model',), key_spec,
                         right_columns=('id', 'model'),
                         post_predicates=(Predicate('right', 'id', 'not_null'),))
    return Scenario('左连接+null拒绝谓词(退化内连接)', join_spec, key_spec, build_spec,
                    dims, LakeTable.from_rows('facts', facts, 1), events,
                    QueryConfig(max_wait=5.0, block_cost=1.0))
