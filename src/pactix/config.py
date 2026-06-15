"""Configuration value objects for pactix.

Durations are :class:`datetime.timedelta`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta


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


@dataclass(frozen=True)
class PactixConfig:
    """Top-level library configuration shared by the outbox and inbox stores."""

    outbox_batch_size: int = 100
    inbox_batch_size: int = 100
    outbox_poll_interval: timedelta = timedelta(milliseconds=250)
    inbox_poll_interval: timedelta = timedelta(milliseconds=250)
    lease_duration: timedelta = timedelta(seconds=30)
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)


__all__ = ['RetryPolicy', 'PactixConfig']
