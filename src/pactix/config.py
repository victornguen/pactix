"""Configuration value objects for pactix.

Durations are :class:`datetime.timedelta`.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import timedelta

from pactix.errors import ValidationError


@dataclass(frozen=True)
class RetryPolicy:
    """Exponential-backoff retry policy.

    ``jitter_factor`` is carried for parity with the upstream crate
    """

    max_attempts: int = 10
    initial_interval: timedelta = timedelta(seconds=1)
    max_interval: timedelta = timedelta(seconds=60)
    multiplier: float = 2.0
    jitter_factor: float = 0.25


class NotifyMode(enum.Enum):
    """How PostgreSQL NOTIFY wakeups are emitted."""

    TRIGGER = 'trigger'
    COALESCED = 'coalesced'
    OFF = 'off'

    def as_str(self) -> str:
        return self.value

    @classmethod
    def from_str(cls, value: str) -> NotifyMode:
        try:
            return cls(value)
        except ValueError as error:
            raise ValidationError(f"unknown notify mode '{value}'") from error


@dataclass(frozen=True)
class WakeupPolicy:
    """Optional wakeup-driven scheduling for the outbox worker (off by default)."""

    enabled: bool = False
    notify_mode: NotifyMode = NotifyMode.COALESCED
    channel: str = 'outbox_wake'
    coalesce_interval: timedelta = timedelta(milliseconds=50)
    fallback_initial_interval: timedelta = timedelta(seconds=1)
    fallback_max_interval: timedelta = timedelta(seconds=60)
    fallback_multiplier: float = 2.0


@dataclass(frozen=True)
class PactixConfig:
    """Top-level library configuration shared by the outbox and inbox stores."""

    outbox_batch_size: int = 100
    inbox_batch_size: int = 100
    outbox_poll_interval: timedelta = timedelta(milliseconds=250)
    inbox_poll_interval: timedelta = timedelta(milliseconds=250)
    lease_duration: timedelta = timedelta(seconds=30)
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    wakeup: WakeupPolicy = field(default_factory=WakeupPolicy)


__all__ = ['RetryPolicy', 'NotifyMode', 'WakeupPolicy', 'PactixConfig']
