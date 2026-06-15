"""Inbox handler protocol, outcomes, and closure adapter.

The borrowed/owned handler split collapses to a single adapter in Python since records are passed by reference.
"""

from __future__ import annotations

import enum
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from .model import InboxMessageRecord


class _Kind(enum.Enum):
    PROCESSED = 'processed'
    RETRYABLE = 'retryable'
    TERMINAL = 'terminal'


@dataclass(frozen=True)
class HandleOutcome:
    """Result of a single inbox handler invocation.

    Build with :meth:`processed`, :meth:`retryable`, or :meth:`terminal`.
    """

    kind: _Kind
    message: str | None = None

    @classmethod
    def processed(cls) -> HandleOutcome:
        return cls(_Kind.PROCESSED)

    @classmethod
    def retryable(cls, message: str) -> HandleOutcome:
        return cls(_Kind.RETRYABLE, message)

    @classmethod
    def terminal(cls, message: str) -> HandleOutcome:
        return cls(_Kind.TERMINAL, message)

    @property
    def is_processed(self) -> bool:
        return self.kind is _Kind.PROCESSED

    @property
    def is_retryable(self) -> bool:
        return self.kind is _Kind.RETRYABLE

    @property
    def is_terminal(self) -> bool:
        return self.kind is _Kind.TERMINAL


@runtime_checkable
class InboxHandler(Protocol):
    """Application business logic invoked after durable receipt."""

    async def handle(self, record: InboxMessageRecord) -> HandleOutcome: ...


HandleFn = Callable[[InboxMessageRecord], Awaitable[HandleOutcome]]


class _FnInboxHandler:
    def __init__(self, handle: HandleFn) -> None:
        self._handle = handle

    async def handle(self, record: InboxMessageRecord) -> HandleOutcome:
        return await self._handle(record)


def inbox_handler(handle: HandleFn) -> InboxHandler:
    """Adapt an async function into an :class:`InboxHandler`."""
    return _FnInboxHandler(handle)


__all__ = ['HandleOutcome', 'InboxHandler', 'inbox_handler']
