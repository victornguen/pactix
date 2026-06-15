"""One-pass inbox worker.

Reclaim and claim run in one transaction;
the stale filter, idempotency hook, and handler run outside any DB transaction;
each state update runs in its own short transaction.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncEngine

from pactix._retry import RetryAction, handle_retryable_failure

from .handler import InboxHandler
from .idempotency import IdempotencyDecision, IdempotencyHook
from .stale import AcceptAllEvents, StaleEventDecision, StaleEventFilter
from .store import InboxStore


def _utcnow() -> datetime:
    return datetime.now(UTC)


class _Mode(enum.Enum):
    FIFO_PER_ORDERING_KEY = 'fifo'
    UNORDERED = 'unordered'


@dataclass
class RunSummary:
    """Aggregate counters from a single :meth:`InboxRunner.run_once`."""

    reclaimed: int = 0
    claimed: int = 0
    processed: int = 0
    stale_skipped: int = 0
    retry_scheduled: int = 0
    failed: int = 0


class InboxRunner:
    """Claims ready rows, applies the processing strategy, and records results."""

    def __init__(
        self,
        store: InboxStore,
        handler: InboxHandler,
        idempotency_hook: IdempotencyHook,
        *,
        mode: _Mode = _Mode.FIFO_PER_ORDERING_KEY,
        stale_event_filter: StaleEventFilter | None = None,
    ) -> None:
        self._store = store
        self._handler = handler
        self._idempotency_hook = idempotency_hook
        self._mode = mode
        self._stale_event_filter: StaleEventFilter = stale_event_filter or AcceptAllEvents()

    @classmethod
    def new_unordered(
        cls,
        store: InboxStore,
        handler: InboxHandler,
        idempotency_hook: IdempotencyHook,
        stale_event_filter: StaleEventFilter,
    ) -> InboxRunner:
        """Build a runner that claims without FIFO gating and drops stale events."""
        return cls(
            store,
            handler,
            idempotency_hook,
            mode=_Mode.UNORDERED,
            stale_event_filter=stale_event_filter,
        )

    async def run_once(self, engine: AsyncEngine) -> RunSummary:
        now = _utcnow()
        async with engine.begin() as conn:
            reclaimed = await self._store.reclaim_expired(conn, now)
            if self._mode is _Mode.UNORDERED:
                claimed_rows = await self._store.claim_ready_unordered(conn, now)
            else:
                claimed_rows = await self._store.claim_ready(conn, now)

        summary = RunSummary(reclaimed=reclaimed, claimed=len(claimed_rows))

        for row in claimed_rows:
            if self._mode is _Mode.UNORDERED:
                decision = await self._stale_event_filter.evaluate(row)
                if decision is StaleEventDecision.STALE:
                    async with engine.begin() as conn:
                        await self._store.mark_processed(conn, row.id, _utcnow())
                    summary.processed += 1
                    summary.stale_skipped += 1
                    continue

            idem = await self._idempotency_hook.evaluate(row)
            if idem is IdempotencyDecision.PROCESS:
                outcome = await self._handler.handle(row)
                if outcome.is_processed:
                    async with engine.begin() as conn:
                        await self._store.mark_processed(conn, row.id, _utcnow())
                    summary.processed += 1
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
            else:  # SKIP or ALREADY_APPLIED
                async with engine.begin() as conn:
                    await self._store.mark_processed(conn, row.id, _utcnow())
                summary.processed += 1

        return summary


__all__ = ['InboxRunner', 'RunSummary']
