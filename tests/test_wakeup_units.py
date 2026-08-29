"""Unit tests for the optional wakeup-driven outbox scheduling (no database)."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from time import monotonic

import pytest

from pactix import (
    CoalescedNotifier,
    FallbackBackoff,
    LocalWakeup,
    NotifyMode,
    PactixConfig,
    PostgresWakeListener,
    ValidationError,
    WakeupPolicy,
    WakeupRunner,
)
from pactix.outbox import RunSummary


class _FakeDialect:
    def __init__(self, name: str) -> None:
        self.name = name


class _NoRealConnectionError(RuntimeError):
    pass


class _FakeEngine:
    """Duck-typed AsyncEngine: records connection attempts, never connects."""

    def __init__(self, dialect_name: str) -> None:
        self.dialect = _FakeDialect(dialect_name)
        self.connect_calls = 0

    def connect(self) -> object:
        self.connect_calls += 1
        raise _NoRealConnectionError('fake engine has no real connections')

    def begin(self) -> object:
        self.connect_calls += 1
        raise _NoRealConnectionError('fake engine has no real connections')

    def raw_connection(self) -> object:
        self.connect_calls += 1
        raise _NoRealConnectionError('fake engine has no real connections')


class _RecordingConnection:
    """Duck-typed AsyncConnection capturing executed statements."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    async def __aenter__(self) -> _RecordingConnection:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    async def execute(self, statement: object, parameters: object = None) -> None:
        self.statements.append(str(statement))

    async def commit(self) -> None:
        return None

    async def rollback(self) -> None:
        return None

    async def close(self) -> None:
        return None


class _RecordingEngine:
    """Duck-typed AsyncEngine handing out a statement-recording connection."""

    def __init__(self, dialect_name: str = 'postgresql') -> None:
        self.dialect = _FakeDialect(dialect_name)
        self.connection = _RecordingConnection()

    def connect(self) -> _RecordingConnection:
        return self.connection

    def begin(self) -> _RecordingConnection:
        return self.connection


class _ScriptedRunner:
    """Duck-typed OutboxRunner returning scripted ``claimed`` counts."""

    def __init__(self, claimed: list[int]) -> None:
        self._claimed = claimed
        self.calls = 0
        self.timestamps: list[float] = []
        self._events: dict[int, asyncio.Event] = {}

    async def run_once(self, engine: object) -> RunSummary:
        self.calls += 1
        self.timestamps.append(monotonic())
        index = min(self.calls - 1, len(self._claimed) - 1)
        for target, event in self._events.items():
            if self.calls >= target:
                event.set()
        return RunSummary(claimed=self._claimed[index])

    async def wait_for_calls(self, target: int, timeout: float = 2.0) -> None:
        event = self._events.setdefault(target, asyncio.Event())
        if self.calls >= target:
            event.set()
        await asyncio.wait_for(event.wait(), timeout)


# --- NotifyMode ------------------------------------------------------------


def test_notify_mode_as_str_values() -> None:
    assert NotifyMode.TRIGGER.as_str() == 'trigger'
    assert NotifyMode.COALESCED.as_str() == 'coalesced'
    assert NotifyMode.OFF.as_str() == 'off'


def test_notify_mode_from_str_round_trip() -> None:
    for mode in NotifyMode:
        assert NotifyMode.from_str(mode.as_str()) is mode


def test_notify_mode_from_str_unknown_raises() -> None:
    with pytest.raises(ValidationError):
        NotifyMode.from_str('polling')


# --- WakeupPolicy / PactixConfig -------------------------------------------


def test_wakeup_policy_defaults() -> None:
    policy = WakeupPolicy()
    assert policy.enabled is False
    assert policy.notify_mode is NotifyMode.COALESCED
    assert policy.channel == 'outbox_wake'
    assert policy.coalesce_interval == timedelta(milliseconds=50)
    assert policy.fallback_initial_interval == timedelta(seconds=1)
    assert policy.fallback_max_interval == timedelta(seconds=60)
    assert policy.fallback_multiplier == 2.0


def test_pactix_config_wakeup_defaults_to_disabled() -> None:
    config = PactixConfig()
    assert config.wakeup == WakeupPolicy()
    assert config.wakeup.enabled is False


# --- FallbackBackoff --------------------------------------------------------


def test_fallback_backoff_starts_at_initial_interval() -> None:
    backoff = FallbackBackoff(WakeupPolicy())
    assert backoff.current_seconds == 1.0


def test_fallback_backoff_idle_doubles_up_to_cap() -> None:
    backoff = FallbackBackoff(WakeupPolicy())
    assert backoff.record_idle() == 2.0
    assert backoff.record_idle() == 4.0
    assert backoff.record_idle() == 8.0
    assert backoff.record_idle() == 16.0
    assert backoff.record_idle() == 32.0
    assert backoff.record_idle() == 60.0  # 64 capped at fallback_max_interval
    assert backoff.record_idle() == 60.0
    assert backoff.current_seconds == 60.0


def test_fallback_backoff_work_found_resets_to_initial() -> None:
    backoff = FallbackBackoff(WakeupPolicy())
    backoff.record_idle()
    backoff.record_idle()
    assert backoff.record_work_found() == 1.0
    assert backoff.current_seconds == 1.0


def test_fallback_backoff_custom_policy() -> None:
    policy = WakeupPolicy(
        fallback_initial_interval=timedelta(milliseconds=250),
        fallback_max_interval=timedelta(seconds=1),
        fallback_multiplier=3.0,
    )
    backoff = FallbackBackoff(policy)
    assert backoff.current_seconds == 0.25
    assert backoff.record_idle() == 0.75
    assert backoff.record_idle() == 1.0  # 2.25 capped at the 1s max
    assert backoff.record_work_found() == 0.25


# --- LocalWakeup ------------------------------------------------------------


async def test_local_wakeup_times_out_without_wake() -> None:
    wakeup = LocalWakeup()
    assert await wakeup.wait(timeout=0.05) is False


async def test_local_wakeup_wake_then_wait_returns_true() -> None:
    wakeup = LocalWakeup()
    wakeup.wake()
    assert await wakeup.wait(timeout=1.0) is True


async def test_local_wakeup_signal_is_consumed_and_coalesced() -> None:
    wakeup = LocalWakeup()
    wakeup.wake()
    wakeup.wake()
    assert await wakeup.wait(timeout=1.0) is True
    assert await wakeup.wait(timeout=0.05) is False  # consumed by the first wait


async def test_local_wakeup_wakes_blocked_waiter() -> None:
    wakeup = LocalWakeup()
    task = asyncio.create_task(wakeup.wait())
    await asyncio.sleep(0.02)  # let the wait park on the event
    wakeup.wake()
    assert await asyncio.wait_for(task, timeout=1.0) is True


# --- CoalescedNotifier -------------------------------------------------------


async def test_coalesced_notifier_debounces() -> None:
    engine = _RecordingEngine()
    notifier = CoalescedNotifier(engine, 'outbox_wake', timedelta(milliseconds=50))
    await notifier.notify()
    await notifier.notify()  # inside the coalesce window -> skipped
    assert len(engine.connection.statements) == 1
    assert 'pg_notify' in engine.connection.statements[0]
    await asyncio.sleep(0.06)  # let the window expire
    await notifier.notify()
    assert len(engine.connection.statements) == 2


# --- PostgresWakeListener ----------------------------------------------------


def test_postgres_wake_listener_constructs_without_connecting() -> None:
    engine = _FakeEngine('postgresql')
    listener = PostgresWakeListener(engine, 'outbox_wake')
    assert listener is not None
    assert engine.connect_calls == 0


# --- WakeupRunner ------------------------------------------------------------


async def test_wakeup_runner_rejects_disabled_policy() -> None:
    engine = _FakeEngine('postgresql')
    runner = _ScriptedRunner([1])
    with pytest.raises(ValidationError):
        await WakeupRunner(runner, WakeupPolicy()).run(engine)
    assert engine.connect_calls == 0
    assert runner.calls == 0


async def test_wakeup_runner_drains_until_empty_on_local_wake() -> None:
    local = LocalWakeup()
    runner = _ScriptedRunner([3, 2, 0])
    policy = WakeupPolicy(
        enabled=True,
        notify_mode=NotifyMode.OFF,
        fallback_initial_interval=timedelta(seconds=60),
    )
    task = asyncio.create_task(WakeupRunner(runner, policy, local_wakeup=local).run(_FakeEngine('mysql')))
    try:
        await asyncio.sleep(0.05)  # let the loop park on the wake wait
        local.wake()
        await runner.wait_for_calls(3, timeout=2.0)  # one wake drains all passes promptly
        await asyncio.sleep(0.1)  # fallback is 60s: nothing more without another wake
        assert runner.calls == 3
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


async def test_wakeup_runner_caps_passes_per_wake() -> None:
    local = LocalWakeup()
    runner = _ScriptedRunner([1])  # never empties -> the 100-pass guard must kick in
    policy = WakeupPolicy(
        enabled=True,
        notify_mode=NotifyMode.OFF,
        fallback_initial_interval=timedelta(seconds=60),
    )
    task = asyncio.create_task(WakeupRunner(runner, policy, local).run(_FakeEngine('mysql')))
    try:
        await asyncio.sleep(0.05)
        local.wake()
        await runner.wait_for_calls(100, timeout=5.0)
        assert runner.calls == 100
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


async def test_wakeup_runner_grows_and_resets_fallback_backoff() -> None:
    runner = _ScriptedRunner([0, 0, 1, 0, 0])
    policy = WakeupPolicy(
        enabled=True,
        notify_mode=NotifyMode.OFF,
        fallback_initial_interval=timedelta(milliseconds=50),
        fallback_multiplier=2.0,
        fallback_max_interval=timedelta(seconds=60),
    )
    start = monotonic()
    task = asyncio.create_task(WakeupRunner(runner, policy).run(_FakeEngine('mysql')))
    try:
        await runner.wait_for_calls(5, timeout=3.0)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    times = runner.timestamps
    # wait-then-drain: the first pass happens only after the initial fallback interval
    assert times[0] - start >= 0.03
    gaps = [times[i + 1] - times[i] for i in range(len(times) - 1)]
    assert gaps[0] >= 0.08  # idle doubled 50ms -> 100ms
    assert gaps[1] >= 0.15  # idle doubled 100ms -> 200ms
    assert gaps[2] <= 0.15  # work found reset the interval back to ~50ms


async def test_wakeup_runner_stops_on_cancel() -> None:
    runner = _ScriptedRunner([0])
    policy = WakeupPolicy(
        enabled=True,
        notify_mode=NotifyMode.OFF,
        fallback_initial_interval=timedelta(milliseconds=20),
    )
    task = asyncio.create_task(WakeupRunner(runner, policy).run(_FakeEngine('mysql')))
    await runner.wait_for_calls(1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_wakeup_runner_activates_listener_on_postgres() -> None:
    engine = _FakeEngine('postgresql')
    runner = _ScriptedRunner([0])
    policy = WakeupPolicy(
        enabled=True,
        notify_mode=NotifyMode.TRIGGER,
        fallback_initial_interval=timedelta(seconds=60),
    )
    task = asyncio.create_task(WakeupRunner(runner, policy).run(engine))
    await asyncio.sleep(0.3)
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, _NoRealConnectionError):
        pass
    # the listener tried to check out a connection on the fake engine
    assert engine.connect_calls >= 1


async def test_wakeup_runner_skips_listener_when_notify_off() -> None:
    engine = _FakeEngine('postgresql')
    runner = _ScriptedRunner([0])
    policy = WakeupPolicy(
        enabled=True,
        notify_mode=NotifyMode.OFF,
        fallback_initial_interval=timedelta(milliseconds=20),
    )
    task = asyncio.create_task(WakeupRunner(runner, policy).run(engine))
    try:
        await runner.wait_for_calls(2, timeout=2.0)  # fallback cycles prove the loop runs
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert engine.connect_calls == 0


async def test_wakeup_runner_skips_listener_on_mysql() -> None:
    engine = _FakeEngine('mysql')
    runner = _ScriptedRunner([0])
    policy = WakeupPolicy(
        enabled=True,
        notify_mode=NotifyMode.COALESCED,
        fallback_initial_interval=timedelta(milliseconds=20),
    )
    task = asyncio.create_task(WakeupRunner(runner, policy).run(engine))
    try:
        await runner.wait_for_calls(2, timeout=2.0)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert engine.connect_calls == 0
