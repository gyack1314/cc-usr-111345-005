"""内存预算：覆盖所有并发过滤器与在途片段，超限即退化并回收，账目可观察。"""
from __future__ import annotations


class MemoryBudget:
    """统一记账：在途片段、已发布快照、并发过滤器都占用同一份预算。"""

    def __init__(self, limit_bytes: int):
        if limit_bytes <= 0:
            raise ValueError('预算必须为正')
        self.limit = limit_bytes
        self._used: dict[str, int] = {}
        self.peak = 0

    @property
    def used(self) -> int:
        return sum(self._used.values())

    @property
    def available(self) -> int:
        return self.limit - self.used

    def try_reserve(self, owner: str, amount: int) -> bool:
        if amount > self.available:
            return False
        self._used[owner] = self._used.get(owner, 0) + amount
        self.peak = max(self.peak, self.used)
        return True

    def release(self, owner: str, amount: int) -> int:
        held = self._used.get(owner, 0)
        freed = min(held, amount)
        if held - freed:
            self._used[owner] = held - freed
        else:
            self._used.pop(owner, None)
        return freed

    def release_all(self, owner: str) -> int:
        return self._used.pop(owner, 0) or 0

    def by_owner(self) -> dict[str, int]:
        return dict(self._used)
