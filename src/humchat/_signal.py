"""Cancellation primitives with JavaScript ``AbortSignal`` semantics.

asyncio task cancellation cannot express "this waiter gives up but the shared work
continues", which the session manager and durable consumers rely on. These small
primitives keep that distinction explicit.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable
from typing import Any, TypeVar

from . import _clock
from .errors import AbortError, HumChatError, TimeoutError

T = TypeVar("T")
Listener = Callable[[Any], None]

_background: set[asyncio.Task[Any]] = set()


class AbortSignal:
    """Observable one-shot cancellation flag. Create one through :class:`AbortController`."""

    __slots__ = ("__weakref__", "_aborted", "_listeners", "_reason")

    def __init__(self) -> None:
        self._aborted = False
        self._reason: Any = None
        self._listeners: list[Listener] = []

    @property
    def aborted(self) -> bool:
        return self._aborted

    @property
    def reason(self) -> Any:
        return self._reason

    def add_listener(self, listener: Listener) -> None:
        if self._aborted:
            return
        self._listeners.append(listener)

    def remove_listener(self, listener: Listener) -> None:
        try:
            self._listeners.remove(listener)
        except ValueError:
            pass

    def throw_if_aborted(self) -> None:
        if self._aborted:
            raise aborted(self)

    def _abort(self, reason: Any) -> None:
        if self._aborted:
            return
        self._aborted = True
        self._reason = reason
        listeners, self._listeners = self._listeners, []
        for listener in listeners:
            try:
                listener(reason)
            except Exception:  # pragma: no cover - listeners are internal and must not raise
                pass

    @staticmethod
    def any(signals: Iterable[AbortSignal]) -> AbortSignal:
        """A signal aborted by the first of ``signals``. Prefer :func:`any_signal` internally."""
        return any_signal(signals)[0]

    @staticmethod
    def timeout(milliseconds: float) -> AbortSignal:
        controller = AbortController()
        _clock.call_later(milliseconds, lambda: controller.abort(TimeoutError("Operation deadline exceeded", retryable=True)))
        return controller.signal


class AbortController:
    """Owner of an :class:`AbortSignal`."""

    __slots__ = ("signal",)

    def __init__(self) -> None:
        self.signal = AbortSignal()

    def abort(self, reason: Any = None) -> None:
        self.signal._abort(AbortError("Operation cancelled") if reason is None else reason)


def any_signal(signals: Iterable[AbortSignal | None]) -> tuple[AbortSignal, Callable[[], None]]:
    """Combine signals; returns the combined signal and a function detaching it from parents."""
    parents = [signal for signal in signals if signal is not None]
    combined = AbortSignal()
    for parent in parents:
        if parent.aborted:
            combined._abort(parent.reason)
            return combined, lambda: None

    def forward(reason: Any) -> None:
        detach()
        combined._abort(reason)

    def detach() -> None:
        for parent in parents:
            parent.remove_listener(forward)

    for parent in parents:
        parent.add_listener(forward)
    return combined, detach


def aborted(signal: AbortSignal) -> HumChatError:
    reason = signal.reason
    return reason if isinstance(reason, HumChatError) else AbortError("Operation cancelled")


class Budget:
    """A deadline combined with parent signals (``budget()`` in the Node SDK)."""

    __slots__ = ("_controller", "_detach", "_timer", "signal")

    def __init__(self, milliseconds: float, signals: Iterable[AbortSignal | None] = ()) -> None:
        self._controller = AbortController()
        self._timer = _clock.call_later(
            milliseconds,
            lambda: self._controller.abort(TimeoutError("Operation deadline exceeded", retryable=True)),
        )
        self.signal, self._detach = any_signal([*signals, self._controller.signal])

    def dispose(self) -> None:
        self._timer.cancel()
        self._detach()


def budget(milliseconds: float, signals: Iterable[AbortSignal | None] = ()) -> Budget:
    return Budget(milliseconds, signals)


def _retrieve(future: asyncio.Future[Any]) -> None:
    if not future.cancelled():
        future.exception()


def ensure_future(value: Awaitable[T]) -> asyncio.Future[T]:
    future = asyncio.ensure_future(value)
    if isinstance(future, asyncio.Task):
        _background.add(future)
        future.add_done_callback(_background.discard)
    return future


def spawn(value: Awaitable[Any]) -> asyncio.Future[Any]:
    """Run detached work, retaining a reference and observing its outcome (``void promise``)."""
    future = ensure_future(value)
    future.add_done_callback(_retrieve)
    return future


async def race_signal(value: Awaitable[T], signal: AbortSignal) -> T:
    """Await ``value`` unless ``signal`` aborts first. The underlying work is never cancelled."""
    future = ensure_future(value)
    future.add_done_callback(_retrieve)
    if signal.aborted:
        raise aborted(signal)
    if future.done():
        return future.result()
    loop = asyncio.get_running_loop()
    waiter: asyncio.Future[None] = loop.create_future()

    def wake(_: Any = None) -> None:
        if not waiter.done():
            waiter.set_result(None)

    signal.add_listener(wake)
    future.add_done_callback(wake)
    try:
        await waiter
    finally:
        signal.remove_listener(wake)
        future.remove_done_callback(wake)
    if future.done():
        return future.result()
    raise aborted(signal)


async def delay(milliseconds: float, signal: AbortSignal) -> None:
    if signal.aborted:
        raise aborted(signal)
    loop = asyncio.get_running_loop()
    waiter: asyncio.Future[bool] = loop.create_future()

    def fire() -> None:
        if not waiter.done():
            waiter.set_result(True)

    def cancel(_: Any = None) -> None:
        if not waiter.done():
            waiter.set_result(False)

    timer = _clock.call_later(milliseconds, fire)
    signal.add_listener(cancel)
    try:
        completed = await waiter
    finally:
        timer.cancel()
        signal.remove_listener(cancel)
    if not completed:
        raise aborted(signal)


async def maybe_await(value: Any) -> Any:
    if asyncio.iscoroutine(value) or isinstance(value, asyncio.Future) or hasattr(value, "__await__"):
        return await value
    return value


__all__ = [
    "AbortController",
    "AbortSignal",
    "Budget",
    "aborted",
    "any_signal",
    "budget",
    "delay",
    "ensure_future",
    "maybe_await",
    "race_signal",
    "spawn",
]
