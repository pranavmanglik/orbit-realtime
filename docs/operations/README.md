# Orbit Realtime: operations

The current Python implementation is an SSE response utility and has no environment variables or
background service of its own. The application supplies an async event iterator and Core/ASGI
serves the response. The app owns any broker connection or subscription that produces events.

## Route and resource ownership

Return `sse_response(events)` only after the route has authenticated and authorized the request.
Scope the source subscription to that user or connection. Ensure the iterator has an async
`aclose()` when it owns resources such as a broker consumer or database cursor; the SSE stream
closes that iterator on response completion or disconnect. If source cleanup fails, the stream
rejects further reads and allows a later `aclose()` call to retry. Keep any surrounding resource
context alive for the full streamed response lifetime.

The iterator is pulled on demand, without package-side prefetch. Avoid adding unbounded buffering
between the event source and response. A slow client may hold one route task and upstream
subscription open for a long time, so define per-user and global connection limits.

## Proxy and timeout configuration

SSE requires intermediaries to pass bytes through incrementally. Configure the chosen reverse
proxy, ingress, CDN, and load balancer to avoid response buffering, allow the expected idle
duration, and close disconnected clients. Header behavior and configuration directives are
provider-specific and are not added by this library beyond the response's content type, no-cache,
and `nosniff` headers.

An idle stream may be closed by network infrastructure. Use `encode_comment()` for application-owned
heartbeats at a suitable interval, and coordinate it with proxy idle timeouts. This package does
not schedule heartbeats or implement a reconnect policy.

## Reconnection and replay

Browsers may reconnect, but this package does not persist events, process `Last-Event-ID`, or replay
missed messages. If replay is needed, the application must validate client cursors, authorize
access, retain a bounded event history, and define behavior for expired cursors. Do not interpret an
arbitrary client-supplied event ID as proof of access or as an unbounded database query.

## Capacity and rollout checks

Each client holds an HTTP stream and can hold an upstream subscription. Size worker and file
descriptor limits accordingly. Test slow readers, abrupt disconnects, source errors, graceful
server shutdown, proxy buffering, keepalive behavior, and reconnect storms in a representative
environment. The package unit tests do not validate ingress configuration or production capacity.
