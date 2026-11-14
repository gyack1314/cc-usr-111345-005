# 星型仓连接的完备片段运行时过滤

本项目保存仓库与湖表开发所需的本地资料，提供内容摘要、记录身份与登记关系检查；并在
`filter_data/engine/` 提供 Python 查询执行层的运行时键过滤：先用维表条件产生候选键，
再扫描事实表，减少事实扫描与中间数据。过滤只减少开销，绝不改变连接袋结果。

资料表包含facts、dimensions、fragments、deliveries。字段单位和关系见 docs/data-contract.md。
data/normal.json登记常规资料，data/boundary.json登记特殊业务情形，data/broken-digest.json
用于检查资料损坏的拒绝行为。JSONL文件按清单内SHA256和字节长度绑定。

运行环境为 Python 3.11及以上，当前仅使用标准库，无需额外服务。包内io.py负责资料读取，
domain.py负责表集合与引用检查，cli.py提供本地检查入口。

```sh
python -m unittest discover -s tests -v
python -m compileall -q filter_data tests
python -m filter_data.cli data/normal.json
python -m filter_data.cli data/boundary.json
```

## 运行时键过滤执行层（filter_data/engine/）

```sh
python -m filter_data.engine.demo     # 全部场景的袋对照与决策溯源证据
```

### 正确性不变式

- **完备性闸门**：只有收齐某一代的全部编号片段、逐一校验身份（执行代/键编码/快照）、
  合并成功且在内存预算内，才发布过滤器快照；只收到一部分键时绝不启用过滤
  （历史事故即"片段晚到时提前启用过滤把真实请求筛没"，`legacy_premature.py`
  仅用于复现对照）。快照发布即不可变，探测侧只能读到已发布快照，因此任何被过滤
  掉的行都可回溯到一个证据完备的构建代（`verify.evidence_problems` 校验）。
- **过滤不替代连接**：过滤器（精确集合或 Bloom，允许假阳性、绝无假阴性）只决定
  "是否必然无连接伙伴"；真实连接始终执行，维表重复键的袋乘数由连接的行袋保留，
  不会被过滤器的集合语义去重。
- **键规范随计划冻结**：复合键编码（类型标签+长度前缀，无歧义）、逐列规范化规则、
  null 相等模式（null_unequal / null_equal）冻结在同一份 KeySpec 中，构建与探测
  一致；encoding_id 是片段身份的一部分。
- **退化只向安全方向**：构建取消/失败/超预算/超时 → 退化为不做过滤完成原查询；
  不存在"错误排除后的成功结果"。

### 下推判定矩阵（plan.analyze_pushdown，依据随决策输出）

| 连接 | 过滤探测侧 | 依据 |
| --- | --- | --- |
| INNER | 允许 | 未匹配行本就不在结果中 |
| SEMI | 允许 | 只输出有匹配的探测行 |
| ANTI | 禁止 | 输出的恰是无匹配行，过滤会删掉结果本身（语义倒置） |
| LEFT（探测=保留侧） | 禁止 | 未匹配行以 null 扩展出现在结果中 |
| LEFT（探测=非保留侧） | 允许 | 非保留侧未匹配行本就不出现 |
| LEFT + 右表 null 拒绝谓词 | 允许 | 退化为内连接语义 |
| LEFT + 右表 IS NULL 谓词 | 禁止 | 退化为反连接语义 |

### 片段、代与合并

- 片段身份绑定（build_id, generation, index/total, encoding_id, snapshot_id）；
  可能乱序（收齐即可）、重复（内容摘要幂等忽略；同序号不同内容 → 冲突，本代失败）、
  超预算（本代失败并回收）、失败/取消（退化）。
- 更高代片段到达视为重试并取代未完成的一代（回收在途内存）；更低代的迟到消息
  一律忽略；已激活后到达的消息不影响已发布的完备快照。
- 分层合并：片段 → 代内合并 → 不可变快照发布。精确集合要求 encoding_id 一致才并；
  Bloom 位图要求（位长, 哈希数, 种子）完全一致才按位 OR，否则拒绝并退化，
  绝不强行 OR 不同种子或不同位长的位图。
- 内存预算统一覆盖在途片段、已发布快照与并发过滤器；超限即退化并回收，
  决策日志记录每一次切换点与回收量。
- 探测侧等待构建侧有界（max_wait），超时不过滤先扫；扫描中途激活也安全——
  已扫块未过滤、后续块过滤，真实连接兜底。成本大于收益时规划期即禁用过滤。

### 演示覆盖

`demo.py` 展示：命中键只在最后一个片段（登记资料包 normal/boundary，含历史缺陷
对照）、跨代迟到消息、Bloom 碰撞（假阳性被真实连接兜住）、null 键两种模式、
保留侧外连接与反连接的禁止及反例、超预算/冲突/取消/超时退化、成本禁用，
以及每次激活的完备证据溯源。

容器内使用同样的检查入口：

```sh
docker build -t filter-data .
docker run --rm filter-data
```
