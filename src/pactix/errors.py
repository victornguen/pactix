"""Error types for pactix.

Three variants as an exception hierarchy: validation, persistence, and routing failures.
"""

from __future__ import annotations


class PactixError(Exception):
    """Base class for all pactix errors."""


class ValidationError(PactixError):
    """A value object or message failed validation."""


class PersistenceError(PactixError):
    """A database operation failed or returned unexpected data."""


class RoutingError(PactixError):
    """No publisher, topic, or endpoint could be resolved for an event type."""


__all__ = [
    'PactixError',
    'ValidationError',
    'PersistenceError',
    'RoutingError',
]
