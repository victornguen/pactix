# Helper adapters

`pactix` exposes its extension points as `typing.Protocol`s and ships small
adapters so you rarely need to write a class.

## Closure adapters

| Helper | Wraps an async function into | Returns |
|---|---|---|
| `outbox_publisher(fn)` | `OutboxPublisher` | `PublishOutcome` |
| `inbox_handler(fn)` | `InboxHandler` | `HandleOutcome` |
| `idempotency_hook(fn)` | `IdempotencyHook` | `IdempotencyDecision` |
| `stale_event_filter(fn)` | `StaleEventFilter` | `StaleEventDecision` |

Each `fn` is `async def fn(record) -> ...` and receives the in-memory record.

```python
from pactix import HandleOutcome, inbox_handler

handler = inbox_handler(lambda record: _handle(record))

async def _handle(record):
    if not valid(record.payload):
        return HandleOutcome.terminal('invalid payload')
    if not dependency_up():
        return HandleOutcome.retryable('billing unavailable')
    await apply(record)
    return HandleOutcome.processed()
```

## Idempotency built-ins

- `ProcessAll()` — forward every pending row to the handler.
- `StaticIdempotency.process() / .skip() / .already_applied()` — a fixed decision
  for every row without writing a custom hook.

```python
from pactix import ProcessAll, StaticIdempotency

InboxRunner(store, handler, ProcessAll())
InboxRunner(store, handler, StaticIdempotency.already_applied())
```

## Stale-event filter (unordered mode only)

- `AcceptAllEvents()` — treat every claimed row as fresh.
- `stale_event_filter(fn)` — drop obsolete rows based on payload version,
  timestamps, or other state.

```python
from pactix import InboxRunner, ProcessAll, StaleEventDecision, stale_event_filter

def is_fresh(record):
    version = record.payload.get('version', 0)
    return StaleEventDecision.FRESH if version >= current_version() else StaleEventDecision.STALE

runner = InboxRunner.new_unordered(
    store, handler, ProcessAll(), stale_event_filter(lambda r: _async(is_fresh(r)))
)
```

A `STALE` decision marks the row processed (counted as both `processed` and
`stale_skipped`) without invoking the hook or handler.

## Publisher registry

`PublisherRegistry` resolves a publisher by `event_type`:

- `register(event_type, publisher)` — exact mapping
- `register_many([...], publisher)` — one publisher for several event types
- `register_all(publisher)` — catch-all used when no exact mapping exists

Exact registrations take precedence over the catch-all. Resolving an unmapped
event type with no catch-all raises `RoutingError`.
