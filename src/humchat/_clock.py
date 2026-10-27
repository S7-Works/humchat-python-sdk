"""Single indirection point for wall time, randomness and timers.

Library code always calls these through the module (``_clock.now_ms()``) so tests can
substitute a virtual clock without patching the event loop.
"""

from __future__ import annotations

import asyncio
import random as _random
import time
from collections.abc import Callable
from typing import Any, Protocol


class TimerHandle(Protocol):
    def cancel(self) -> None: ...


def now_ms() -> float:
    """Unix wall-clock milliseconds (JavaScript ``Date.now()`` equivalent)."""
    return time.time() * 1000.0


def random() -> float:
    return _random.random()


def call_later(milliseconds: float, callback: Callable[[], Any]) -> TimerHandle:
    """Schedule ``callback`` on the running loop after ``milliseconds``."""
    loop = asyncio.get_running_loop()
    return loop.call_later(max(0.0, milliseconds) / 1000.0, callback)


class _Interval:
    def __init__(self, milliseconds: float, callback: Callable[[], Any]) -> None:
        self._milliseconds = milliseconds
        self._callback = callback
        self._cancelled = False
        self._handle: TimerHandle | None = None
        self._schedule()

    def _schedule(self) -> None:
        if not self._cancelled:
            self._handle = call_later(self._milliseconds, self._fire)

    def _fire(self) -> None:
        if self._cancelled:
            return
        self._schedule()
        self._callback()

    def cancel(self) -> None:
        self._cancelled = True
        if self._handle is not None:
            self._handle.cancel()
            self._handle = None


def call_every(milliseconds: float, callback: Callable[[], Any]) -> TimerHandle:
    """Repeating timer (``setInterval`` equivalent)."""
    return _Interval(milliseconds, callback)
