"""把登记资料包适配为执行层场景。

资料包约定（docs/data-contract.md）：
- dimensions.fragment 标记构建侧片段编号，fragments.total 是冻结片段全集大小；
- dimensions 中 model 重复是合法维表袋记录（袋乘数）；
- deliveries.order 是本地调度顺序，文件顺序不代表业务事件顺序；
- facts 含 null 键行，是否匹配由查询键模式（NullMode）决定。
"""
from __future__ import annotations

from ..domain import inspect_tables
from ..io import load
from .coordinator import BuildSpec
from .executor import LakeTable, TransportEvent
from .fragments import Fragment, FragmentId
from .harness import QueryConfig, Scenario
from .keys import KeySpec, NullMode
from .plan import JoinSpec, JoinType

BUILD_ID = 'dim_model'
SNAPSHOT_ID = 'pack-v1'
DIM_COLUMNS = ('id', 'model', 'selected', 'fragment')


def scenario_from_pack(path: str, *, join_type: JoinType = JoinType.INNER,
                       null_mode: NullMode = NullMode.NULL_UNEQUAL,
                       label: str | None = None,
                       config: QueryConfig | None = None) -> Scenario:
    tables = load(path)
    inspect_tables(tables)
    facts = tables['facts']
    dims = [d for d in tables['dimensions'] if d['selected']]
    frag_rows = {f['id']: f for f in tables['fragments']}

    key_spec = KeySpec(arity=1, normalizations=((),), null_mode=null_mode)
    totals = {f['total'] for f in frag_rows.values()}
    if len(totals) != 1:
        raise ValueError('资料包片段全集大小不一致')
    build_spec = BuildSpec(build_id=BUILD_ID, snapshot_id=SNAPSHOT_ID,
                           num_fragments=totals.pop(), filter_kind='exact')

    events = []
    # order 是调度顺序：必须按 order 排序，而不是文件顺序
    for delivery in sorted(tables['deliveries'], key=lambda r: r['order']):
        frag_row = frag_rows[delivery['fragment']]
        index = frag_row['index']
        if frag_row['status'] != 'complete':
            events.append(TransportEvent(at=float(delivery['order']), action='fail',
                                         generation=frag_row['generation'],
                                         reason=f"status:{frag_row['status']}"))
            continue
        keys = [key_spec.encode_values((d['model'],))
                for d in dims if d['fragment'] == index]
        fid = FragmentId(build_id=BUILD_ID, generation=frag_row['generation'],
                         index=index, total=frag_row['total'],
                         encoding_id=key_spec.encoding_id, snapshot_id=SNAPSHOT_ID)
        fragment = Fragment.from_keys(fid, [k for k in keys if k is not None])
        events.append(TransportEvent(at=float(delivery['order']), action='deliver',
                                     fragment=fragment))

    join_spec = JoinSpec(join_type=join_type, left_keys=('model',),
                         right_keys=('model',), key_spec=key_spec,
                         right_columns=DIM_COLUMNS)
    return Scenario(
        label=label or f'{path}({join_type.value})',
        join_spec=join_spec,
        key_spec=key_spec,
        build_spec=build_spec,
        build_rows=dims,
        probe_table=LakeTable.from_rows('facts', facts, rows_per_block=1),
        events=events,
        config=config if config is not None else QueryConfig(max_wait=0.0, block_cost=2.0),
    )
