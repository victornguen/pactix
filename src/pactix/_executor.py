"""Shared executor typing.

The outbox/inbox stores accept a SQLAlchemy executor supplied by the caller so
durable writes can join the caller's open transaction. Both ``AsyncConnection``
and ``AsyncSession`` satisfy the :class:`Executor` protocol. The runners instead
own an ``AsyncEngine`` and manage their own short-lived connections per pass.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

from sqlalchemy import Executable
from sqlalchemy.engine import Result


class Executor(Protocol):
    """Anything that can execute a SQLAlchemy statement asynchronously.

    Satisfied by ``sqlalchemy.ext.asyncio.AsyncConnection`` and
    ``AsyncSession``.
    """

    async def execute(
        self,
        statement: Executable,
        parameters: Mapping[str, Any] | None = ...,
        *args: Any,
        **kwargs: Any,
    ) -> Result[Any]: ...


__all__ = ['Executor']
