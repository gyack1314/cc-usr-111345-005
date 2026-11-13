# 业务资料约定

dimensions.fragment标记构建侧片段编号，total是冻结片段全集大小。model重复是合法维表袋记录，null键是否匹配由查询键模式决定。deliveries.order为本地调度顺序，非业务时间。

每行id在该表内唯一，文件顺序不代表业务事件顺序。清单schema=1，各表登记文件名、字节长度、SHA256、行数及完整字段集合，未知版本或内容变化应重新登记。业务身份、金额、版本及测量数值均为本地资料，不代表任何现实机构的经营数据。

- facts：id、model、tenant。
- dimensions：id、model、selected、fragment。
- fragments：id、generation、index、total、status。
- deliveries：id、fragment、order。

normal与boundary是相互独立的资料包，不能直接拼接重复id。引用字段按本项目domain.py的REFERENCES登记。读取检查只检查完整性、身份及这些直接引用，不替代仓库查询语义、快照可见性或递归完备性判断。
