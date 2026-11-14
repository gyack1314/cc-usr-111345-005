"""构建侧片段：身份绑定执行代、键编码与快照；内容摘要判定重复。"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass

from .filters import BloomFilter, ExactSetFilter


@dataclass(frozen=True)
class FragmentId:
    """片段身份：执行代 generation、键编码 encoding_id、快照 snapshot_id 三元绑定。"""

    build_id: str
    generation: int
    index: int
    total: int
    encoding_id: str
    snapshot_id: str


@dataclass(frozen=True)
class Fragment:
    """一个编号片段的不可变载荷。checksum 覆盖身份与内容，用于重复判定。"""

    fid: FragmentId
    kind: str  # 'exact' | 'bloom'
    payload: bytes
    checksum: str
    approx_bytes: int

    @staticmethod
    def _checksum(fid: FragmentId, kind: str, payload: bytes) -> str:
        digest = hashlib.sha256()
        digest.update(repr(fid).encode('utf-8'))
        digest.update(kind.encode('ascii'))
        digest.update(payload)
        return digest.hexdigest()

    @classmethod
    def from_keys(cls, fid: FragmentId, keys, approx_bytes: int | None = None) -> 'Fragment':
        filt = ExactSetFilter(fid.encoding_id)
        for key in keys:
            filt.add(key)
        payload = filt.serialize()
        return cls(fid, ExactSetFilter.KIND, payload,
                   cls._checksum(fid, ExactSetFilter.KIND, payload),
                   approx_bytes if approx_bytes is not None else len(payload))

    @classmethod
    def from_bloom(cls, fid: FragmentId, bloom: BloomFilter,
                   approx_bytes: int | None = None) -> 'Fragment':
        payload = bloom.serialize()
        return cls(fid, BloomFilter.KIND, payload,
                   cls._checksum(fid, BloomFilter.KIND, payload),
                   approx_bytes if approx_bytes is not None else len(payload))

    def decode_exact(self) -> ExactSetFilter:
        return ExactSetFilter.deserialize(self.payload, self.fid.encoding_id)

    def decode_bloom(self) -> BloomFilter:
        return BloomFilter.deserialize(self.payload)
