# Orbit Realtime: architecture and boundaries

## Implemented capability

The Python package currently implements Server-Sent Events (SSE) framing and a native Orbit ASGI
stream response. It does not provide a WebSocket server, WebRTC signaling/media stack,
WebTransport, cross-process event fanout, or a broker. Do not assume those capabilities are
implemented because they appear on a roadmap or in another repository.

```text
Orbit route ── async iterator[SSEEvent] ── SSEStream ── Orbit Core Response/ASGI server
                         pull one event at a time; no package-side queue
```

Core owns HTTP routing, the ASGI application/server, and the response lifecycle. `orbit-realtime`
formats events and gives the route an ASGI response; it does not add another web framework or
server. The caller owns the upstream event iterator and any broker subscription behind it.

Separate Go (`orbit-realtime-go`) and Rust (`orbit-realtime-rust`) implementations expose the SSE
framing capability for their native HTTP runtimes. They do not introduce a process boundary or
gRPC protocol for this operation. Keep language-specific server integration in the matching
package.

## Wire framing and bounds

`SSEEvent` encodes optional `event`, `id`, and `retry` fields followed by one or more `data` fields
and a blank line. CR and CRLF in data become separate data lines. Field-specific line breaks and
NUL in IDs are rejected to prevent frame injection. Event data is limited to a 65,536-byte UTF-8
frame; event names and IDs have smaller bounds. `encode_comment` emits a separately bounded comment
frame, suitable for an explicit heartbeat.

The response sets `text/event-stream; charset=utf-8`, `Cache-Control: no-cache`, and
`X-Content-Type-Options: nosniff`. It does not set proxy-specific buffering controls because those
vary by ingress. Applications must configure reverse proxies and load balancers to preserve
streaming and suitable idle timeouts.

## Backpressure and cleanup

`SSEStream` requests and encodes one event each time the response consumer asks for the next byte
chunk. It does not prefetch or maintain an Orbit-side queue. This allows backpressure to flow to the
caller iterator, subject to buffering in Core, the ASGI server, and network infrastructure.

The stream is single-consumer. It closes a closeable upstream iterator on EOF, invalid event,
encoding failure, response cancellation, or explicit `aclose()`. Concurrent close calls share one
cleanup task, and cancellation of a close caller is propagated only after source cleanup finishes.
If source cleanup fails or is cancelled, the stream remains unavailable and a later `aclose()`
retries it. The route remains responsible for closing any separate broker subscriptions or
resources that feed its iterator.

## Security and application responsibilities

SSE is a long-lived HTTP response, not an authorization mechanism. Authenticate and authorize
before returning the response and ensure each event is authorized for that connection. Do not put
secrets or sensitive payloads in event IDs or logs. The event data is text; callers are responsible
for safe encoding/escaping at the eventual browser rendering boundary.

This package does not persist events, replay `Last-Event-ID`, manage retention, send automatic
heartbeats, bound the number of clients, or provide fanout. Add explicit application controls for
connection quotas, source subscription limits, heartbeat intervals, reconnect behavior, and
per-client authorization as appropriate.

## Source layout and status

- `src/orbit_realtime/sse.py`: validated SSE values, encoder, iterator lifecycle, and Core response.
- `src/orbit_realtime/__init__.py`: public imports.

The Python package is in alpha and declares Python `>=3.11,<3.15` with `orbit-core>=0.1.0a1,<0.2`.
