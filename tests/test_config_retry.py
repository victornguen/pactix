"""Tests for the retry-and-config capability."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from pactix._retry import RetryAction, handle_retryable_failure, next_retry_at
from pactix.config import PactixConfig, RetryPolicy


def test_default_retry_policy() -> None:
    policy = RetryPolicy()
    assert policy.max_attempts == 10
    assert policy.initial_interval == timedelta(seconds=1)
    assert policy.max_interval == timedelta(seconds=60)
    assert policy.multiplier == 2.0
    assert policy.jitter_factor == 0.25


def test_default_config() -> None:
    config = PactixConfig()
    assert config.outbox_batch_size == 100
    assert config.inbox_batch_size == 100
    assert config.outbox_poll_interval == timedelta(milliseconds=250)
    assert config.inbox_poll_interval == timedelta(milliseconds=250)
    assert config.lease_duration == timedelta(seconds=30)
    assert config.retry_policy == RetryPolicy()


def test_exponential_growth_capped_at_maximum() -> None:
    policy = RetryPolicy()
    now = datetime(2026, 1, 1, tzinfo=UTC)
    d0 = next_retry_at(policy, 0, now) - now
    d1 = next_retry_at(policy, 1, now) - now
    d2 = next_retry_at(policy, 2, now) - now
    assert d0 == timedelta(seconds=1)
    assert d1 == timedelta(seconds=2)
    assert d2 == timedelta(seconds=4)
    # large attempt count is capped at max_interval
    assert next_retry_at(policy, 100, now) - now == timedelta(seconds=60)


class _RecordingStore:
    def __init__(self) -> None:
        self.retry_policy = RetryPolicy(max_attempts=3)
        self.retried: list[tuple[object, str, datetime]] = []
        self.failed: list[tuple[object, str]] = []

    async def mark_retry(self, executor: object, id: object, error: str, available_at: datetime) -> None:
        self.retried.append((id, error, available_at))

    async def mark_failed(self, executor: object, id: object, error: str) -> None:
        self.failed.append((id, error))


@pytest.mark.asyncio
async def test_schedule_retry_below_ceiling() -> None:
    store = _RecordingStore()
    now = datetime(2026, 1, 1, tzinfo=UTC)
    action = await handle_retryable_failure(store, None, 'id-1', 0, 3, 'boom', now)
    assert action is RetryAction.RETRY_SCHEDULED
    assert store.retried and not store.failed
    assert store.retried[0][2] == now + timedelta(seconds=1)


@pytest.mark.asyncio
async def test_fail_at_ceiling() -> None:
    store = _RecordingStore()
    now = datetime(2026, 1, 1, tzinfo=UTC)
    # attempts=2, next would be 3 == max_attempts -> fail
    action = await handle_retryable_failure(store, None, 'id-1', 2, 3, 'boom', now)
    assert action is RetryAction.FAILED
    assert store.failed and not store.retried
