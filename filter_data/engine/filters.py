"""候选键过滤器：精确集合或 Bloom。允许假阳性，绝不允许假阴性。

合并规则（分层合并的硬约束）：
- 精确集合：仅当键编码 encoding_id 一致才可求并；
- Bloom 位图：仅当 (位长, 哈希数, 种子) 完全一致才可按位 OR。
  不同 hash 种子或不同位长的位图直接 OR 会破坏"无假阴性"性质，
  一律拒绝（FilterParamsMismatch），由协调器把该代标记失败并退化。
"""
from __future__ import annotations

import hashlib
import struct

from .keys import KeySpec


class FilterParamsMismatch(ValueError):
    """位图参数（种子/位长/哈希数）或键编码不一致，禁止合并。"""


def _hash_pair(seed: int, key: bytes) -> tuple[int, int]:
    digest = hashlib.sha256(struct.pack('>Q', seed) + key).digest()
    h1 = int.from_bytes(digest[:16], 'big')
    h2 = int.from_bytes(digest[16:], 'big') | 1  # 保证步长非零
    return h1, h2


class BloomFilter:
    """双哈希 Bloom 位图：只增不删，无假阴性。"""

    KIND = 'bloom'

    def __init__(self, num_bits: int, num_hashes: int, seed: int):
        if num_bits < 8 or num_hashes < 1 or not (0 <= seed < 1 << 64):
            raise ValueError('Bloom 参数无效')
        self.num_bits = num_bits
        self.num_hashes = num_hashes
        self.seed = seed
        self._bits = bytearray((num_bits + 7) // 8)

    @property
    def params(self) -> tuple[int, int, int]:
        return (self.num_bits, self.num_hashes, self.seed)

    @property
    def memory_bytes(self) -> int:
        return len(self._bits)

    def _positions(self, key: bytes):
        h1, h2 = _hash_pair(self.seed, key)
        for i in range(self.num_hashes):
            yield (h1 + i * h2) % self.num_bits

    def add(self, key: bytes) -> None:
        for pos in self._positions(key):
            self._bits[pos >> 3] |= 1 << (pos & 7)

    def might_contain(self, key: bytes) -> bool:
        return all((self._bits[pos >> 3] >> (pos & 7)) & 1 for pos in self._positions(key))

    def merge(self, other: 'BloomFilter') -> None:
        if self.params != other.params:
            raise FilterParamsMismatch(
                f'位图参数不一致，禁止直接 OR: {self.params} vs {other.params}')
        for i, byte in enumerate(other._bits):
            self._bits[i] |= byte

    def serialize(self) -> bytes:
        header = struct.pack('>4sIII', b'BLM1', self.num_bits, self.num_hashes, self.seed)
        return header + bytes(self._bits)

    @classmethod
    def deserialize(cls, payload: bytes) -> 'BloomFilter':
        if len(payload) < 16:
            raise ValueError('Bloom 载荷过短')
        magic, num_bits, num_hashes, seed = struct.unpack('>4sIII', payload[:16])
        if magic != b'BLM1':
            raise ValueError('Bloom 载荷魔数不符')
        filt = cls(num_bits, num_hashes, seed)
        body = payload[16:]
        if len(body) != len(filt._bits):
            raise ValueError('Bloom 载荷长度与参数不符')
        filt._bits[:] = body
        return filt


class ExactSetFilter:
    """编码键的精确集合：零假阳性，内存随键数线性增长。"""

    KIND = 'exact'

    def __init__(self, encoding_id: str):
        self.encoding_id = encoding_id
        self._keys: set[bytes] = set()

    def __len__(self) -> int:
        return len(self._keys)

    @property
    def memory_bytes(self) -> int:
        return 64 + sum(len(k) + 16 for k in self._keys)

    def add(self, key: bytes) -> None:
        self._keys.add(key)

    def might_contain(self, key: bytes) -> bool:
        return key in self._keys

    def merge(self, other: 'ExactSetFilter') -> None:
        if self.encoding_id != other.encoding_id:
            raise FilterParamsMismatch('键编码不一致，禁止合并精确集合')
        self._keys |= other._keys

    def serialize(self) -> bytes:
        out = [struct.pack('>I', len(self._keys))]
        for key in sorted(self._keys):
            out.append(struct.pack('>I', len(key)))
            out.append(key)
        return b''.join(out)

    @classmethod
    def deserialize(cls, payload: bytes, encoding_id: str) -> 'ExactSetFilter':
        if len(payload) < 4:
            raise ValueError('精确集合载荷过短')
        (count,) = struct.unpack('>I', payload[:4])
        filt = cls(encoding_id)
        offset = 4
        for _ in range(count):
            (size,) = struct.unpack('>I', payload[offset:offset + 4])
            offset += 4
            filt._keys.add(payload[offset:offset + size])
            offset += size
        if offset != len(payload):
            raise ValueError('精确集合载荷有尾部垃圾')
        return filt


class KeyFilterView:
    """键规范 + 过滤器 + 探测列 → 行级判定。

    might_match 回答"是否可能存在连接伙伴"。返回 False 必须是无假阴性的
    必然结论：null_unequal 下含 null 的键必然无匹配；否则查过滤器。
    """

    def __init__(self, spec: KeySpec, filt, probe_columns: tuple[str, ...]):
        self.spec = spec
        self.filt = filt
        self.probe_columns = probe_columns

    def might_match(self, row: dict) -> bool:
        encoded = self.spec.encode_row(row, self.probe_columns)
        if encoded is None:
            return False  # null_unequal：含 null 分量的键必然无连接伙伴
        return self.filt.might_contain(encoded)


def add_row(spec: KeySpec, filt, row: dict, columns: tuple[str, ...]) -> bool:
    """把构建侧一行键加入过滤器。null_unequal 下的 null 键不插入（必然无匹配）。"""
    encoded = spec.encode_row(row, columns)
    if encoded is None:
        return False
    filt.add(encoded)
    return True
