# Orbit Realtime: security

SSE keeps an HTTP request open and sends text frames over it. The protocol itself does not
authenticate clients, authorize events, encrypt data outside HTTPS, or protect browser rendering.
The application, Core deployment, and its network edge provide those controls.

## Authenticate before streaming

Authenticate and authorize each connection before returning the SSE response. Apply tenant and
resource permissions to the event source so a long-lived connection cannot continue receiving
data after its authorization expires or is revoked. Decide how existing streams are closed when
credentials expire or account access changes.

Enforce connection quotas per user/tenant and globally. A client can keep a request and source
subscription open for an extended period. Limit event source fanout and avoid creating a new
unbounded broker consumer for every connection.

## Frame and browser boundaries

`SSEEvent` rejects line breaks in event names and IDs, rejects NUL in IDs, and splits data line
breaks into separate `data:` fields. Encoded frames are bounded. Still treat event data as
untrusted text: when browser code renders it, use text APIs or context-appropriate escaping rather
than inserting it as HTML.

Use HTTPS and configure CORS and cookie policy for the actual application. Do not put bearer tokens
or other secrets in event IDs, query strings, or payloads. `Last-Event-ID` and reconnect cursors are
client-controlled values; this package does not validate them against a store or enforce access.

## Logging and intermediaries

Event data and IDs are excluded from dataclass representations, but applications and servers may
log response data independently. Avoid logging full event payloads, personal data, session
identifiers, or sensitive query parameters. Configure proxies to protect the stream and prevent
buffering; proxy access logs should follow the same data minimization policy.

## Vulnerability reporting

Follow the repository [security policy](../../SECURITY.md) for private vulnerability reporting.
This page describes the utility's security boundary and is not an application or deployment audit.
