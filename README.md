# Orbit Realtime

`orbit-realtime` currently provides bounded Server-Sent Events framing over Orbit Core's native
ASGI streaming response. It does not add a web framework or a second ASGI server. It supports
Python 3.11 through 3.14 and is alpha.

```bash
python -m pip install orbit-realtime
```

## Server-Sent Events

Create `SSEEvent` values and pass an async iterator to `sse_response`. The response pulls one event
at a time, so Core and the ASGI server apply stream backpressure without an Orbit-side event queue:

```python
from collections.abc import AsyncIterator

from orbit.asgi import Request, Response
from orbit_realtime import SSEEvent, sse_response


async def updates() -> AsyncIterator[SSEEvent]:
    async for change in subscribe_to_changes():
        yield SSEEvent(
            event="record.changed",
            id=str(change.revision),
            data=change.to_json(),
        )


async def changes(request: Request) -> Response:
    return sse_response(updates())
```

Event payloads are UTF-8 text with a 64 KiB encoded-frame limit. Newlines are represented as
separate `data:` fields; optional event type, ID, and bounded reconnect delay follow the HTML
standard event-stream format. Omit optional fields with `None`; an explicit empty event name emits
`event:` and selects the default type, while an explicit empty ID emits `id:` to reset browser
reconnect state. Go and Rust implementations preserve the same distinction. Event payloads and IDs are excluded from representations. The response
sets the `text/event-stream` media type, disables cache reuse, and closes the upstream iterator when
the stream ends, fails, is cancelled, or the client disconnects. Use `encode_comment()` for explicit
heartbeat frames when the source needs to keep an idle connection open.

Authentication, authorization, replay storage, event retention, heartbeats, reconnect policy, and
per-client fanout remain application responsibilities. Use per-user authorization and bound any
upstream subscription resources. The package does not yet implement WebSockets or a shared pub/sub
backend; the stream itself only forwards events from the caller's async iterator.

## Documentation

The package-specific guides cover [architecture](docs/architecture/overview.md), [operations and security](docs/operations/README.md), and [development](docs/development/README.md), with [security guidance](docs/security/overview.md). The [documentation index](docs/README.md) links to the full package overview and project policies.

## Development

```bash
python -m pip install -e '.[dev]'
pytest
ruff check .
mypy
```

Licensed under Apache-2.0.


### Go and Rust alternatives

The SSE capability also has independently installable language implementations: [orbit-realtime-go](https://github.com/orbit-projects/orbit-realtime-go) uses the standard `net/http` streaming API, and [orbit-realtime-rust](https://github.com/orbit-projects/orbit-realtime-rust) implements the standard `http-body` contract. Both preserve bounded event framing and pull/write backpressure without introducing a cross-process protocol.
