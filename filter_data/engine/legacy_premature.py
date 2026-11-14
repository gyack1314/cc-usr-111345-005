"""历史缺陷复现：收到首个片段即把部分键集当全集发布过滤器。

仅用于对照演示与回归测试，复现"片段晚到时提前启用过滤，把真实请求
筛没"的事故机理。禁止用于生产路径——安全实现见 coordinator.py，
那里只有收齐一代全部编号片段（完备证据）才允许发布。
"""
from __future__ import annotations

from .coordinator import BuildSideCoordinator, FilterSnapshot, GenState
from .filters import KeyFilterView


class LegacyPrematureCoordinator(BuildSideCoordinator):
    """缺陷版本：第一个片段到达就激活过滤器（部分键集冒充全集）。"""

    def receive(self, frag):
        result = super().receive(frag)
        if result == 'accepted' and self._snapshot is None:
            self._activate_partial()
        return result

    def _activate_partial(self) -> None:
        merged, key_count = self._merge_received()
        generation = self._generation
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
            complete=False,  # 缺陷标记：证据不完备的部分快照
        )
        self._state = GenState.ACTIVE
        self.log.add(self.clock.now, self.spec.build_id, 'filter_activated',
                     generation=generation, snapshot_id=self.spec.snapshot_id,
                     encoding_id=self.key_spec.encoding_id,
                     total=self.spec.num_fragments,
                     evidence=[list(item) for item in evidence],
                     key_count=key_count, memory_bytes=merged.memory_bytes,
                     complete=False)
