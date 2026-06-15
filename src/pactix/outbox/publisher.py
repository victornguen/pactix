"""Outbox publish outcomes, publisher protocol, adapters, and registry."""

from __future__ import annotations

import asyncio
import enum
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from pactix.errors import RoutingError

from .model import OutboxMessageRecord


class _Kind(enum.Enum):
    PUBLISHED = 'published'
    RETRYABLE = 'retryable'
    TERMINAL = 'terminal'


@dataclass(frozen=True)
class PublishOutcome:
    """Result of a single publish attempt.

    Build with :meth:`published`, :meth:`retryable`, or :meth:`terminal`.
    """

    kind: _Kind
    message: str | None = None

    @classmethod
    def published(cls) -> PublishOutcome:
        return cls(_Kind.PUBLISHED)

    @classmethod
    def retryable(cls, message: str) -> PublishOutcome:
        return cls(_Kind.RETRYABLE, message)

    @classmethod
    def terminal(cls, message: str) -> PublishOutcome:
        return cls(_Kind.TERMINAL, message)

    @property
    def is_published(self) -> bool:
        return self.kind is _Kind.PUBLISHED

    @property
    def is_retryable(self) -> bool:
        return self.kind is _Kind.RETRYABLE

    @property
    def is_terminal(self) -> bool:
        return self.kind is _Kind.TERMINAL


@runtime_checkable
class OutboxPublisher(Protocol):
    """Application callback used by ``OutboxRunner`` to publish a claimed row."""

    async def publish(self, record: OutboxMessageRecord) -> PublishOutcome: ...


PublishFn = Callable[[OutboxMessageRecord], Awaitable[PublishOutcome]]


class _FnOutboxPublisher:
    def __init__(self, publish: PublishFn) -> None:
        self._publish = publish

    async def publish(self, record: OutboxMessageRecord) -> PublishOutcome:
        return await self._publish(record)


def outbox_publisher(publish: PublishFn) -> OutboxPublisher:
    """Adapt an async function into an :class:`OutboxPublisher`."""
    return _FnOutboxPublisher(publish)


class PublisherRegistry:
    """Resolves an :class:`OutboxPublisher` by ``event_type``.

    Exact registrations take precedence over a catch-all publisher.
    """

    def __init__(self) -> None:
        self._publishers: dict[str, OutboxPublisher] = {}
        self._catch_all: OutboxPublisher | None = None
        self._lock = asyncio.Lock()

    async def register(self, event_type: str, publisher: OutboxPublisher) -> None:
        async with self._lock:
            self._publishers[event_type] = publisher

    async def register_many(self, event_types: Iterable[str], publisher: OutboxPublisher) -> None:
        async with self._lock:
            for event_type in event_types:
                self._publishers[event_type] = publisher

    async def register_all(self, publisher: OutboxPublisher) -> None:
        """Register a catch-all publisher used when no exact mapping exists."""
        async with self._lock:
            self._catch_all = publisher

    async def resolve(self, event_type: str) -> OutboxPublisher:
        async with self._lock:
            publisher = self._publishers.get(event_type) or self._catch_all
        if publisher is None:
            raise RoutingError(f"no publisher registered for '{event_type}'")
        return publisher

    async def publish(self, record: OutboxMessageRecord) -> PublishOutcome:
        publisher = await self.resolve(record.event_type)
        return await publisher.publish(record)


__all__ = [
    'PublishOutcome',
    'OutboxPublisher',
    'outbox_publisher',
    'PublisherRegistry',
]
