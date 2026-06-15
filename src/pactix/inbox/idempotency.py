"""Inbox idempotency-hook protocol, decisions, and built-ins."""

from __future__ import annotations

import enum
from collections.abc import Awaitable, Callable
from typing import Protocol, runtime_checkable

from .model import InboxMessageRecord


class IdempotencyDecision(enum.Enum):
    """Decision returned before the handler runs."""

    PROCESS = 'process'
    SKIP = 'skip'
    ALREADY_APPLIED = 'already_applied'


@runtime_checkable
class IdempotencyHook(Protocol):
    """Decides whether a message still needs business processing."""

    async def evaluate(self, record: InboxMessageRecord) -> IdempotencyDecision: ...


EvaluateFn = Callable[[InboxMessageRecord], Awaitable[IdempotencyDecision]]


class _FnIdempotencyHook:
    def __init__(self, evaluate: EvaluateFn) -> None:
        self._evaluate = evaluate

    async def evaluate(self, record: InboxMessageRecord) -> IdempotencyDecision:
        return await self._evaluate(record)


def idempotency_hook(evaluate: EvaluateFn) -> IdempotencyHook:
    """Adapt an async function into an :class:`IdempotencyHook`."""
    return _FnIdempotencyHook(evaluate)


class StaticIdempotency:
    """An idempotency hook that returns a fixed decision for every row."""

    def __init__(self, decision: IdempotencyDecision) -> None:
        self._decision = decision

    @classmethod
    def process(cls) -> StaticIdempotency:
        return cls(IdempotencyDecision.PROCESS)

    @classmethod
    def skip(cls) -> StaticIdempotency:
        return cls(IdempotencyDecision.SKIP)

    @classmethod
    def already_applied(cls) -> StaticIdempotency:
        return cls(IdempotencyDecision.ALREADY_APPLIED)

    async def evaluate(self, record: InboxMessageRecord) -> IdempotencyDecision:
        return self._decision


class ProcessAll:
    """Idempotency hook that forwards every pending row to the handler."""

    async def evaluate(self, record: InboxMessageRecord) -> IdempotencyDecision:
        return IdempotencyDecision.PROCESS


__all__ = [
    'IdempotencyDecision',
    'IdempotencyHook',
    'idempotency_hook',
    'StaticIdempotency',
    'ProcessAll',
]
