"""Stale-event-filter protocol, decisions, and built-in.

Used only in unordered processing mode.
"""

from __future__ import annotations

import enum
from collections.abc import Awaitable, Callable
from typing import Protocol, runtime_checkable

from .model import InboxMessageRecord


class StaleEventDecision(enum.Enum):
    """Decision returned by a stale-event filter."""

    FRESH = 'fresh'
    STALE = 'stale'


@runtime_checkable
class StaleEventFilter(Protocol):
    """Drops obsolete events before business processing in unordered mode."""

    async def evaluate(self, record: InboxMessageRecord) -> StaleEventDecision: ...


EvaluateFn = Callable[[InboxMessageRecord], Awaitable[StaleEventDecision]]


class _FnStaleEventFilter:
    def __init__(self, evaluate: EvaluateFn) -> None:
        self._evaluate = evaluate

    async def evaluate(self, record: InboxMessageRecord) -> StaleEventDecision:
        return await self._evaluate(record)


def stale_event_filter(evaluate: EvaluateFn) -> StaleEventFilter:
    """Adapt an async function into a :class:`StaleEventFilter`."""
    return _FnStaleEventFilter(evaluate)


class AcceptAllEvents:
    """Stale filter that treats every claimed row as fresh."""

    async def evaluate(self, record: InboxMessageRecord) -> StaleEventDecision:
        return StaleEventDecision.FRESH


__all__ = [
    'StaleEventDecision',
    'StaleEventFilter',
    'stale_event_filter',
    'AcceptAllEvents',
]
