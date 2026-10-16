# Copyright 2026-present Orbit Contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""SSE wire encoding, response headers, backpressure, and upstream cleanup tests."""

from __future__ import annotations

import asyncio

import pytest
from orbit.asgi import Response

from orbit_realtime import SSEEvent, SSEStream, encode_comment, sse_response


def test_event_serializes_optional_fields_and_multiline_data() -> None:
    event = SSEEvent(data="first\r\nsecond\rthird", event="record.changed", id="rev-7", retry=500)

    assert event.encode() == (
        b"event: record.changed\nid: rev-7\nretry: 500\ndata: first\ndata: second\ndata: third\n\n"
    )


@pytest.mark.parametrize(
    ("event", "expected"),
    [("", b"event: \ndata: value\n\n"), (None, b"data: value\n\n")],
)
def test_empty_or_omitted_event_type(event: str | None, expected: bytes) -> None:
    assert SSEEvent(data="value", event=event).encode() == expected


def test_empty_id_resets_last_event_id_and_zero_retry_is_valid() -> None:
    assert SSEEvent(data="", id="", retry=0).encode() == b"id: \nretry: 0\ndata: \n\n"


def test_comment_encodes_heartbeat_and_multiline_values() -> None:
    assert encode_comment("connected\r\nwaiting") == b": connected\n: waiting\n\n"
    assert encode_comment() == b": \n\n"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"data": 1},
        {"data": "x" * (65_536 + 1)},
        {"data": "\ud800"},
        {"data": "value", "event": "bad\nevent"},
        {"data": "value", "id": "bad\x00id"},
        {"data": "value", "id": "bad\nid"},
        {"data": "value", "retry": True},
        {"data": "value", "retry": -1},
        {"data": "value", "retry": 2_147_483_648},
    ],
)
def test_event_rejects_invalid_types_fields_and_bounds(kwargs: dict[str, object]) -> None:
    with pytest.raises((TypeError, ValueError)):
        SSEEvent(**kwargs)  # type: ignore[arg-type]


async def test_comment_has_a_separate_encoded_size_limit() -> None:
    with pytest.raises(ValueError, match="comment"):
        encode_comment("x" * 4_097)


async def test_response_streams_on_demand_and_has_safe_sse_headers() -> None:
    requested = 0

    async def events():
        nonlocal requested
        requested += 1
        yield SSEEvent(data="one")
        requested += 1
        yield SSEEvent(data="two")

    response = sse_response(events())
    assert isinstance(response, Response)
    assert response.headers.get("content-type") == "text/event-stream; charset=utf-8"
    assert response.headers.get("cache-control") == "no-cache"
    assert response.headers.get("x-content-type-options") == "nosniff"
    assert response.headers.get("content-length") is None
    assert isinstance(response.stream, SSEStream)
    assert requested == 0

    assert await anext(response.stream) == b"data: one\n\n"
    assert requested == 1
    await response.stream.aclose()


async def test_response_stream_closes_upstream_on_normal_completion() -> None:
    closed = asyncio.Event()

    async def events():
        try:
            yield SSEEvent(data="one")
        finally:
            closed.set()

    stream = SSEStream(events())
    assert await anext(stream) == b"data: one\n\n"
    with pytest.raises(StopAsyncIteration):
        await anext(stream)
    assert closed.is_set()
    await stream.aclose()


async def test_response_stream_closes_upstream_after_disconnect_cancellation() -> None:
    closed = asyncio.Event()
    wait = asyncio.Event()

    async def events():
        try:
            await wait.wait()
            yield SSEEvent(data="late")
        finally:
            closed.set()

    stream = SSEStream(events())
    pending = asyncio.create_task(anext(stream))
    await asyncio.sleep(0)
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    assert closed.is_set()
    assert await stream.aclose() is None


async def test_stream_cleanup_is_shared_and_finishes_before_cancellation_propagates() -> None:
    """A cancelled closer cannot strand or duplicate an upstream resource cleanup."""
    started = asyncio.Event()
    release = asyncio.Event()

    class Source:
        def __init__(self) -> None:
            self.close_calls = 0

        def __aiter__(self) -> Source:
            return self

        async def __anext__(self) -> SSEEvent:
            await asyncio.Event().wait()
            raise StopAsyncIteration

        async def aclose(self) -> None:
            self.close_calls += 1
            started.set()
            await release.wait()

    source = Source()
    stream = SSEStream(source)
    cancelled = asyncio.create_task(stream.aclose())
    await started.wait()
    waiter = asyncio.create_task(stream.aclose())
    cancelled.cancel()
    await asyncio.sleep(0)
    assert not cancelled.done()
    assert not waiter.done()

    release.set()
    with pytest.raises(asyncio.CancelledError):
        await cancelled
    await waiter
    assert source.close_calls == 1


@pytest.mark.asyncio
async def test_failed_source_cleanup_can_be_retried() -> None:
    """A source close failure remains retryable while the stream rejects further reads."""

    class FailOnceSource:
        def __init__(self) -> None:
            self.close_calls = 0

        def __aiter__(self) -> FailOnceSource:
            return self

        async def __anext__(self) -> SSEEvent:
            raise StopAsyncIteration

        async def aclose(self) -> None:
            self.close_calls += 1
            if self.close_calls == 1:
                raise RuntimeError("temporary source close failure")

    source = FailOnceSource()
    stream = SSEStream(source)
    with pytest.raises(RuntimeError, match="temporary source close failure"):
        await stream.aclose()

    with pytest.raises(StopAsyncIteration):
        await anext(stream)
    await stream.aclose()
    await stream.aclose()
    assert source.close_calls == 2


async def test_invalid_source_value_closes_upstream_before_failing() -> None:
    closed = asyncio.Event()

    async def events():
        try:
            yield "not an event"  # type: ignore[misc]
        finally:
            closed.set()

    stream = SSEStream(events())  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="SSEEvent"):
        await anext(stream)
    assert closed.is_set()


@pytest.mark.parametrize("status", [101, 204, 205, 304, 600, True, 200.0])
def test_response_rejects_status_that_cannot_carry_an_sse_body(status: object) -> None:
    async def events():
        yield SSEEvent(data="unused")

    with pytest.raises(ValueError):
        sse_response(events(), status=status)  # type: ignore[arg-type]
