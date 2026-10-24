"""Text analysis and BM25 scoring that mimic Lucene (the search library under Atlas Search), standard library only.

Known approximations: the tokenizer is a `\\w+` split plus lowercase, not UAX#29; fuzzy expansion
is simplified; collection statistics are exact for live documents, while Lucene also counts deleted documents
until segments merge.
"""

from __future__ import annotations

import math
import re
import struct

K1 = 1.2
B = 0.75

_TOKEN = re.compile(r"\w+", re.UNICODE)


def tokenize(text: str) -> list[str]:
    return _TOKEN.findall(text.lower())


# ---- Lucene SmallFloat.intToByte4 / byte4ToInt: field length is stored as one lossy byte -------------------


def _long_to_int4(i: int) -> int:
    num_bits = i.bit_length()
    if num_bits < 4:
        return i
    shift = num_bits - 4
    return ((i >> shift) & 0x07) | ((shift + 1) << 3)


def _int4_to_long(i: int) -> int:
    bits = i & 0x07
    shift = (i >> 3) - 1
    if shift >= 0:
        bits |= 0x08
        bits <<= shift
    return bits


_MAX_INT4 = _long_to_int4(2**31 - 1)
_NUM_FREE = 255 - _MAX_INT4


def quantize_length(length: int) -> int:
    """Length as Lucene's norm byte decodes it."""
    b = length if length < _NUM_FREE else _NUM_FREE + _long_to_int4(length - _NUM_FREE)
    return b if b < _NUM_FREE else _NUM_FREE + _int4_to_long(b - _NUM_FREE)


def idf(doc_count: int, doc_freq: int) -> float:
    return math.log(1.0 + (doc_count - doc_freq + 0.5) / (doc_freq + 0.5))


_F32 = struct.Struct("f")


def f32(x: float) -> float:
    """Round to float32: Lucene scores are single precision, and ties only line up if we round the same way."""
    return _F32.unpack(_F32.pack(x))[0]


def bm25(idf_value: float, freq: float, length: int, avg_len: float) -> float:
    dl = quantize_length(length)
    return f32(f32(idf_value) * freq / (freq + K1 * (1.0 - B + B * dl / f32(avg_len))))


# ---- fuzzy matching: Damerau-Levenshtein (optimal string alignment) as in Lucene FuzzyQuery(transpositions) ---


def edit_distance(a: str, b: str, limit: int) -> int:
    if abs(len(a) - len(b)) > limit:
        return limit + 1
    prev2: list[int] | None = None
    prev = list(range(len(b) + 1))
    for i in range(1, len(a) + 1):
        cur = [i] + [0] * len(b)
        for j in range(1, len(b) + 1):
            cost = 0 if a[i - 1] == b[j - 1] else 1
            cur[j] = min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + cost)
            if prev2 is not None and i > 1 and j > 1 and a[i - 1] == b[j - 2] and a[i - 2] == b[j - 1]:
                cur[j] = min(cur[j], prev2[j - 2] + 1)
        prev2, prev = prev, cur
    return prev[len(b)]
