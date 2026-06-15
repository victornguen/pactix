# HTTP transport

Optional, behind the `http` extra (`pip install "pactix[http]"`). The outbox side
uses `httpx`; the inbox side uses `starlette`. Each imports its framework lazily,
so you can install and use one side without the other. The core library never
imports either.

Both sides share the canonical metadata headers (`x-pactix-event-type`,
`x-pactix-message-id`, `x-pactix-message-key`, `x-pactix-ordering-key`,
`x-pactix-correlation-id`).

## Outbox → HTTP

Map event types to endpoints and use `HttpOutboxPublisher`. The request body is
the record payload as JSON; the metadata headers are attached automatically.

```python
from pactix.transport.http import (
    HttpEndpoint,
    HttpEndpointRouter,
    HttpOutboxPublisher,
    HttpxRequestClient,
)

router = HttpEndpointRouter()
await router.register('order.created', HttpEndpoint('POST', 'https://billing/events'))

publisher = HttpOutboxPublisher(HttpxRequestClient(), router)
await registry.register('order.created', publisher)
```

Responses are classified: `2xx` → `Published`; `408`, `429`, and `5xx` →
`Retryable`; everything else → `Terminal`. A transport-level send error is
`Retryable`.

## HTTP → inbox

Build a Starlette route (or endpoint function) that durably stores inbound events
before any business processing.

```python
from starlette.applications import Starlette

from pactix.transport.http import inbox_route

app = Starlette(routes=[
    inbox_route('/events', source='http:orders', store=inbox_store, engine=engine),
])
```

A valid request returns `202 Accepted`. A missing required header (`message-id`
or `event-type`) returns `400`; a persistence/routing failure returns `500`.
