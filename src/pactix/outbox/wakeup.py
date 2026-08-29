"""Optional wakeup-driven scheduling for the outbox worker.

Merges three wake sources so the worker reacts in milliseconds instead of
polling blindly: an in-process post-commit signal, PostgreSQL LISTEN/NOTIFY,
and adaptive fallback polling as the safety net. Off by default
(``WakeupPolicy.enabled``); the service still owns scheduling, the library
only exposes the building blocks and an opt-in loop.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any, Protocol, cast

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from pactix.config import NotifyMode, WakeupPolicy
from pactix.errors import ValidationError

from .runner import OutboxRunner

# Guard against an infinite drain loop when the queue never empties.
_MAX_PASSES_PER_WAKE = 100

# asyncpg ships no ``py.typed`` marker, so the driver connection is typed
# structurally instead of importing it.
_NotifyCallback = Callable[..., None]


class _DriverConnection(Protocol):
    """The part of the asyncpg driver connection the wake listener relies on."""

    def add_listener(self, channel: str, callback: _NotifyCallback) -> Awaitable[None]: ...

    def remove_listener(self, channel: str, callback: _NotifyCallback) -> Awaitable[None]: ...


class LocalWakeup:
    """In-process post-commit wake signal (asyncio.Event-based)."""

    def __init__(self) -> None:
        self._event = asyncio.Event()

    def wake(self) -> None:
        """Signal the worker; cheap, sync, safe to call right after commit."""
        self._event.set()

    async def wait(self, timeout: float | None = None) -> bool:
        """Wait for the signal; True if woken, False on timeout.

        The signal is consumed either way.
        """
        try:
            await asyncio.wait_for(self._event.wait(), timeout)
        except TimeoutError:
            self._event.clear()
            return False
        self._event.clear()
        return True


class FallbackBackoff:
    """Adaptive fallback polling schedule: reset on work, x multiplier when idle, capped."""

    def __init__(self, policy: WakeupPolicy) -> None:
        self._initial_seconds = policy.fallback_initial_interval.total_seconds()
        self._max_seconds = policy.fallback_max_interval.total_seconds()
        self._multiplier = policy.fallback_multiplier
        self._current_seconds = self._initial_seconds

    @property
    def current_seconds(self) -> float:
        return self._current_seconds

    def record_work_found(self) -> float:
        """Reset to ``fallback_initial_interval`` and return the new seconds."""
        self._current_seconds = self._initial_seconds
        return self._current_seconds

    def record_idle(self) -> float:
        """Multiply by ``fallback_multiplier``, cap at the max, and return the new seconds."""
        self._current_seconds = min(self._current_seconds * self._multiplier, self._max_seconds)
        return self._current_seconds


class PostgresWakeListener:
    """LISTEN on the wake channel via a dedicated checked-out asyncpg connection.

    The connection is checked out of the pool on the first :meth:`wait` and
    stays checked out until :meth:`aclose`, so pool sizing must account for
    it. LISTEN/NOTIFY is session-based: behind PgBouncer in transaction
    pooling mode the server session can change between transactions and
    notifications are silently lost — use session pooling or a direct
    connection there.
    """

    def __init__(self, engine: AsyncEngine, channel: str) -> None:
        self._engine = engine
        self._channel = channel
        self._conn: AsyncConnection | None = None
        self._event = asyncio.Event()

    def _on_notify(self, *args: Any) -> None:
        self._event.set()

    async def _ensure_listener(self) -> None:
        if self._conn is not None:
            return
        conn = await self._engine.connect()
        try:
            raw = await conn.get_raw_connection()
            driver = cast(_DriverConnection, raw.driver_connection)
            await driver.add_listener(self._channel, self._on_notify)
        except BaseException:
            await conn.close()
            raise
        self._conn = conn

    async def wait(self, timeout: float | None = None) -> bool:
        """Wait for a NOTIFY on the channel; True if notified, False on timeout."""
        await self._ensure_listener()
        try:
            await asyncio.wait_for(self._event.wait(), timeout)
        except TimeoutError:
            self._event.clear()
            return False
        self._event.clear()
        return True

    async def aclose(self) -> None:
        """Remove the listener and return the connection to the pool."""
        conn, self._conn = self._conn, None
        if conn is None:
            return
        try:
            raw = await conn.get_raw_connection()
            driver = cast(_DriverConnection, raw.driver_connection)
            await driver.remove_listener(self._channel, self._on_notify)
        finally:
            await conn.close()


class CoalescedNotifier:
    """App-level debounced ``SELECT pg_notify(:channel, '')`` for COALESCED mode."""

    def __init__(self, engine: AsyncEngine, channel: str, coalesce_interval: timedelta) -> None:
        self._engine = engine
        self._channel = channel
        self._coalesce_seconds = coalesce_interval.total_seconds()
        self._last_notified: float | None = None

    async def notify(self) -> None:
        """Emit the NOTIFY unless the last successful one is too recent (monotonic clock)."""
        now = time.monotonic()
        if self._last_notified is not None and now - self._last_notified < self._coalesce_seconds:
            return
        await self._execute_notify()
        self._last_notified = now

    async def _execute_notify(self) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(text("SELECT pg_notify(:channel, '')"), {'channel': self._channel})


class WakeupRunner:
    """Opt-in wakeup-driven loop: merges local/notify/fallback signals, drains via the runner."""

    def __init__(
        self,
        runner: OutboxRunner,
        policy: WakeupPolicy,
        local_wakeup: LocalWakeup | None = None,
    ) -> None:
        self._runner = runner
        self._policy = policy
        self._local_wakeup = local_wakeup

    async def run(self, engine: AsyncEngine) -> None:
        """Loop until cancelled; ``CancelledError`` propagates.

        On each wake the outbox is drained until empty, then the fallback
        schedule is reset if any pass claimed rows and backed off otherwise.
        """
        if not self._policy.enabled:
            raise ValidationError('wakeup policy is disabled')
        backoff = FallbackBackoff(self._policy)
        listener = self._make_listener(engine)
        try:
            while True:
                await self._wait_for_wake(listener, backoff.current_seconds)
                if await self._drain(engine):
                    backoff.record_work_found()
                else:
                    backoff.record_idle()
        finally:
            if listener is not None:
                await listener.aclose()

    def _make_listener(self, engine: AsyncEngine) -> PostgresWakeListener | None:
        if engine.dialect.name != 'postgresql' or self._policy.notify_mode is NotifyMode.OFF:
            return None
        return PostgresWakeListener(engine, self._policy.channel)

    async def _wait_for_wake(self, listener: PostgresWakeListener | None, timeout: float) -> None:
        """Wait for the first of the local signal, a NOTIFY, or the fallback timeout."""
        waiters: list[Awaitable[bool]] = []
        if self._local_wakeup is not None:
            waiters.append(self._local_wakeup.wait())
        if listener is not None:
            waiters.append(listener.wait())
        if not waiters:
            await asyncio.sleep(timeout)
            return
        tasks = [asyncio.ensure_future(waiter) for waiter in waiters]
        try:
            done, _ = await asyncio.wait(tasks, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _drain(self, engine: AsyncEngine) -> bool:
        """Run passes until no rows are claimed; True if any pass claimed rows."""
        found_work = False
        for _ in range(_MAX_PASSES_PER_WAKE):
            summary = await self._runner.run_once(engine)
            if summary.claimed == 0:
                break
            found_work = True
        return found_work


__all__ = [
    'LocalWakeup',
    'FallbackBackoff',
    'PostgresWakeListener',
    'CoalescedNotifier',
    'WakeupRunner',
]
