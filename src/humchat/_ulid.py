"""Monotonic ULID generation (the ``ulid`` package's ``monotonicFactory`` equivalent)."""

from __future__ import annotations

import os
import threading

from . import _clock

_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
_RANDOM_MAX = (1 << 80) - 1


def _encode(value: int, length: int) -> str:
    chars = []
    for _ in range(length):
        chars.append(_ALPHABET[value & 31])
        value >>= 5
    return "".join(reversed(chars))


class MonotonicUlid:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last_time = -1
        self._last_random = 0

    def __call__(self) -> str:
        with self._lock:
            now = int(_clock.now_ms())
            if now <= self._last_time:
                now = self._last_time
                if self._last_random >= _RANDOM_MAX:
                    raise RuntimeError("ULID random component overflow")
                self._last_random += 1
            else:
                self._last_time = now
                self._last_random = int.from_bytes(os.urandom(10), "big")
            return _encode(now, 10) + _encode(self._last_random, 16)


next_id = MonotonicUlid()
