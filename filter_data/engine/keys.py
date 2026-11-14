"""连接键规范：复合键编码、规范化规则与 null 相等模式，随计划冻结。

键规范（KeySpec）是计划的一部分：构建侧与探测侧必须使用同一份规范，
否则编码结果不可比。规范的 encoding_id 是片段身份的一部分，编码规则
不同的键不允许进入同一个过滤器。
"""
from __future__ import annotations

import hashlib
import struct
from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterable


class NullMode(Enum):
    """null 键相等模式，随计划冻结。"""

    NULL_UNEQUAL = 'null_unequal'  # SQL 默认：NULL 与任何键（含 NULL）都不相等
    NULL_EQUAL = 'null_equal'  # IS NOT DISTINCT FROM：NULL 与 NULL 相等


_NORMALIZERS = {
    'strip': str.strip,
    'lower': str.lower,
    'upper': str.upper,
}

ENCODING_VERSION = 'K1'


def normalize_value(value: Any, rules: Iterable[str]) -> Any:
    """按冻结规则链规范化单个键分量（仅作用于字符串）。"""
    for rule in rules:
        fn = _NORMALIZERS.get(rule)
        if fn is None:
            raise ValueError(f'未知规范化规则: {rule}')
        if isinstance(value, str):
            value = fn(value)
    return value


def _encode_component(value: Any) -> bytes:
    """单个分量的自描述编码：类型标签 + 长度前缀，拼接后无歧义。"""
    if value is None:
        return b'N'
    if isinstance(value, bool):
        return b'B' + (b'\x01' if value else b'\x00')
    if isinstance(value, int):
        return b'I' + struct.pack('>q', value)
    if isinstance(value, float):
        return b'F' + struct.pack('>d', value)
    if isinstance(value, str):
        raw = value.encode('utf-8')
        return b'S' + struct.pack('>I', len(raw)) + raw
    raise TypeError(f'不支持的键分量类型: {type(value)!r}')


@dataclass(frozen=True)
class KeySpec:
    """随计划冻结的等值键规范。

    - arity：键列数（支持复合键）；
    - normalizations：按键列位置给出的规范化规则链；
    - null_mode：null 键是否可能匹配，构建与探测必须一致。
    """

    arity: int
    normalizations: tuple[tuple[str, ...], ...]
    null_mode: NullMode = NullMode.NULL_UNEQUAL

    def __post_init__(self):
        if self.arity < 1:
            raise ValueError('键至少一列')
        if len(self.normalizations) != self.arity:
            raise ValueError('规范化规则数与键列数不一致')

    @property
    def encoding_id(self) -> str:
        """规范的内容摘要：编码规则不同则 id 不同，片段据此拒绝混入。"""
        raw = repr((ENCODING_VERSION, self.arity, self.normalizations, self.null_mode.value))
        return hashlib.sha256(raw.encode('utf-8')).hexdigest()[:16]

    def normalize_values(self, values: tuple) -> tuple:
        if len(values) != self.arity:
            raise ValueError('键值数量与规范不一致')
        return tuple(normalize_value(v, self.normalizations[i]) for i, v in enumerate(values))

    def encode_values(self, values: tuple) -> bytes | None:
        """规范化并编码一行键。

        null_unequal 模式下任一分量为 None → 返回 None，表示"无键"：
        该键在连接中必然无匹配（NULL 不等于任何值）。
        null_equal 模式下 None 是普通键值，编码为 N 分量，可匹配 None。
        """
        values = self.normalize_values(values)
        if self.null_mode is NullMode.NULL_UNEQUAL and any(v is None for v in values):
            return None
        parts = [ENCODING_VERSION.encode('ascii')]
        for value in values:
            parts.append(_encode_component(value))
        return b''.join(parts)

    def encode_row(self, row: dict, columns: tuple[str, ...]) -> bytes | None:
        return self.encode_values(tuple(row.get(c) for c in columns))
