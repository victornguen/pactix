"""Shared retry scheduling and the retry-or-fail decision.

Both the outbox and inbox runners route retryable
failures through :func:`handle_retryable_failure` so they back off identically.
"""

from __future__ import annotations

import enum
import math
import uuid
from datetime import datetime, timedelta
from typing import Protocol

from pactix._executor import Executor
from pactix.config import RetryPolicy


class RetryAction(enum.Enum):
    """Outcome of :func:`handle_retryable_failure`."""

    RETRY_SCHEDULED = 'retry_scheduled'
    FAILED = 'failed'


class RetryStore(Protocol):
    """Persistence operations the retry decision needs from a store."""

    @property
    def retry_policy(self) -> RetryPolicy: ...

    async def mark_retry(
        self,
        executor: Executor,
        id: uuid.UUID,
        error: str,
        available_at: datetime,
    ) -> None: ...

    async def mark_failed(self, executor: Executor, id: uuid.UUID, error: str) -> None: ...


def next_retry_at(retry_policy: RetryPolicy, attempts: int, now: datetime) -> datetime:
    """Compute the next retry time for ``attempts`` already made.

    ``now + min(initial_interval * multiplier**attempts, max_interval)``. No
    jitter is applied, matching the upstream crate. Computed in float seconds so
    large attempt counts saturate to ``max_interval`` instead of overflowing.
    """
    initial_seconds = retry_policy.initial_interval.total_seconds()
    max_seconds = retry_policy.max_interval.total_seconds()
    try:
        scaled_seconds = initial_seconds * (retry_policy.multiplier**attempts)
    except OverflowError:
        scaled_seconds = math.inf
    capped_seconds = min(scaled_seconds, max_seconds)
    return now + timedelta(seconds=capped_seconds)


async def handle_retryable_failure(
    store: RetryStore,
    executor: Executor,
    id: uuid.UUID,
    attempts: int,
    max_attempts: int,
    error: str,
    now: datetime,
) -> RetryAction:
    """Reschedule the record for retry, or mark it failed at the attempt ceiling.

    If the next attempt would reach ``max_attempts`` the record is marked
    failed; otherwise it is returned to ``pending`` with a future
    ``available_at``.
    """
    if attempts + 1 >= max_attempts:
        await store.mark_failed(executor, id, error)
        return RetryAction.FAILED

    retry_at = next_retry_at(store.retry_policy, attempts, now)
    await store.mark_retry(executor, id, error, retry_at)
    return RetryAction.RETRY_SCHEDULED


__all__ = ['RetryAction', 'RetryStore', 'next_retry_at', 'handle_retryable_failure']
