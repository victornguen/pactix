"""One-pass outbox worker.

Claim and reclaim run in one transaction;
the publisher runs outside any DB transaction; each state update runs in its own
short transaction so leases stay meaningful across slow publisher I/O.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncEngine

from pactix._retry import RetryAction, handle_retryable_failure

from .publisher import PublisherRegistry
from .store import OutboxStore


def _utcnow() -> datetime:
    return datetime.now(UTC)


@dataclass
class RunSummary:
    """Aggregate counters from a single :meth:`OutboxRunner.run_once`."""

    reclaimed: int = 0
    claimed: int = 0
    published: int = 0
    retry_scheduled: int = 0
    failed: int = 0


class OutboxRunner:
    """Claims ready rows, publishes them, and records the result."""

    def __init__(self, store: OutboxStore, publishers: PublisherRegistry) -> None:
        self._store = store
        self._publishers = publishers

    async def run_once(self, engine: AsyncEngine) -> RunSummary:
        now = _utcnow()
        async with engine.begin() as conn:
            reclaimed = await self._store.reclaim_expired(conn, now)
            claimed_rows = await self._store.claim_ready(conn, now)

        summary = RunSummary(reclaimed=reclaimed, claimed=len(claimed_rows))

        for row in claimed_rows:
            outcome = await self._publishers.publish(row)
            if outcome.is_published:
                async with engine.begin() as conn:
                    await self._store.mark_published(conn, row.id, _utcnow())
                summary.published += 1
            elif outcome.is_retryable:
                async with engine.begin() as conn:
                    action = await handle_retryable_failure(
                        self._store,
                        conn,
                        row.id,
                        row.attempts,
                        row.max_attempts,
                        outcome.message or '',
                        _utcnow(),
                    )
                if action is RetryAction.RETRY_SCHEDULED:
                    summary.retry_scheduled += 1
                else:
                    summary.failed += 1
            else:  # terminal
                async with engine.begin() as conn:
                    await self._store.mark_failed(conn, row.id, outcome.message or '')
                summary.failed += 1

        return summary


__all__ = ['OutboxRunner', 'RunSummary']
