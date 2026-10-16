# Orbit Realtime: development

The Python package currently contains SSE framing and native Orbit ASGI integration. Keep the
implementation compatible with Core's existing `Response` stream contract and avoid adding a
second server or hidden buffering layer.

## Local setup and checks

From the repository root:

```bash
python -m pip install -e '.[dev]' build
pytest
ruff check src tests
ruff format --check src tests
mypy
python -m build
```

Unit tests exercise byte framing, field validation, size limits, pull-based iteration, response
headers, source cleanup, and cancellation without opening a network listener. They do not verify
browser reconnect behavior, ASGI server shutdown, proxies, or deployment capacity.

## Implementation guidance

- Keep the source iterator single-consumer and avoid prefetch queues that defeat backpressure.
- Preserve source cleanup on exhaustion, error, cancellation, and client disconnect. If close can
  block, ensure cancellation cannot leave a resource unreleased and concurrent close callers join
  the same cleanup.
- Keep UTF-8 frame bounds and reject delimiter injection in control fields. Add boundary tests when
  changing validation or encoding.
- Never include event data or IDs in `repr`, routine errors, or logs.
- Update Python, Go, and Rust package docs together when describing cross-language wire parity; run
  each language's own formatter, tests, and static checks in its repository.

Follow the repository contribution guide, governance, code of conduct, and security policy. Keep
fixtures synthetic and report exact local checks separately from hosted CI or live ingress tests.
