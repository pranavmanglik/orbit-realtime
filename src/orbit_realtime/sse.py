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
"""Server-Sent Events framing and native Orbit ASGI response integration."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

from orbit.asgi import Response

_MAX_EVENT_BYTES = 65_536
_MAX_EVENT_NAME_BYTES = 256
_MAX_EVENT_ID_BYTES = 1_024
_MAX_COMMENT_BYTES = 4_096
_MAX_RETRY_MS = 2_147_483_647


@dataclass(frozen=True, slots=True)
class SSEEvent:
    """One bounded event; payload and event ID are excluded from representations."""

    data: str = field(repr=False)
    event: str | None = None
    id: str | None = field(default=None, repr=False)
    retry: int | None = None

    def __post_init__(self) -> None:
        """Validate field types and bounds without changing event data contents."""
        _validate_text(self.data, "data", _MAX_EVENT_BYTES)
        if self.event is not None:
            _validate_text(self.event, "event", _MAX_EVENT_NAME_BYTES, line_safe=True)
        if self.id is not None:
            _validate_text(self.id, "id", _MAX_EVENT_ID_BYTES, line_safe=True, reject_null=True)
        if self.retry is not None and (
            isinstance(self.retry, bool)
            or not isinstance(self.retry, int)
            or not 0 <= self.retry <= _MAX_RETRY_MS
        ):
            raise ValueError(
                f"retry must be an integer between 0 and {_MAX_RETRY_MS} milliseconds."
            )

    def encode(self) -> bytes:
        """Serialize fields according to the SSE wire format, ending with a blank line."""
        fields: list[str] = []
        if self.event is not None:
            fields.append(f"event: {self.event}")
        if self.id is not None:
            fields.append(f"id: {self.id}")
        if self.retry is not None:
            fields.append(f"retry: {self.retry}")
        normalized = self.data.replace("\r\n", "\n").replace("\r", "\n")
        fields.extend(f"data: {line}" for line in normalized.split("\n"))
        encoded = ("\n".join(fields) + "\n\n").encode("utf-8")
        if len(encoded) > _MAX_EVENT_BYTES:
            raise ValueError("encoded event exceeds the 65536-byte limit.")
        return encoded


def encode_comment(comment: str = "") -> bytes:
    """Encode an SSE comment frame, commonly used by callers as a keepalive."""
    _validate_text(comment, "comment", _MAX_COMMENT_BYTES)
    normalized = comment.replace("\r\n", "\n").replace("\r", "\n")
    encoded = ("\n".join(f": {line}" for line in normalized.split("\n")) + "\n\n").encode("utf-8")
    if len(encoded) > _MAX_COMMENT_BYTES:
        raise ValueError("encoded comment exceeds the 4096-byte limit.")
    return encoded


class SSEStream(AsyncIterator[bytes]):
    """Encode events on demand and close the caller-owned async iterator with the response."""

    def __init__(self, events: AsyncIterator[SSEEvent]) -> None:
        """Wrap one asynchronous source without prefetching or buffering its events."""
        try:
            iterator = aiter(events)
        except (AttributeError, TypeError):
            raise TypeError("events must be an asynchronous iterator of SSEEvent values.") from None
        if not callable(getattr(iterator, "__anext__", None)):
            raise TypeError("events must be an asynchronous iterator of SSEEvent values.")
        close = getattr(iterator, "aclose", None)
        if close is not None and (
            not callable(close)
            or not (inspect.iscoroutinefunction(close) or inspect.isasyncgen(iterator))
        ):
            raise TypeError("event iterator cleanup must be an async callable.")
        self._events = iterator
        self._closed = False
        self._close_task: asyncio.Task[None] | None = None

    def __aiter__(self) -> SSEStream:
        """Return this single-consumer asynchronous iterator."""
        return self

    async def __anext__(self) -> bytes:
        """Fetch and encode one event, closing upstream on end or error."""
        if self._closed:
            raise StopAsyncIteration
        try:
            event = await anext(self._events)
        except StopAsyncIteration:
            await self.aclose()
            raise
        except BaseException:
            await self.aclose()
            raise
        if not isinstance(event, SSEEvent):
            await self.aclose()
            raise TypeError("event source must yield SSEEvent values.")
        try:
            return event.encode()
        except BaseException:
            await self.aclose()
            raise

    async def aclose(self) -> None:
        """Close the upstream iterator once and finish cleanup before propagating cancellation.

        The first caller creates the close task; concurrent callers join it. Shielding keeps a
        cancelled response task from abandoning an async generator's ``finally`` block or another
        source-specific resource release. Cancellation is re-raised after the shared close task
        finishes. If the source close fails or is cancelled, a later call starts a new cleanup
        attempt while the stream remains unavailable for reads.
        """
        task = self._close_task
        retry_failed_close = (
            task is not None and task.done() and (task.cancelled() or task.exception() is not None)
        )
        if task is None or retry_failed_close:
            self._closed = True
            close = getattr(self._events, "aclose", None)
            if close is None:
                self._close_task = None
                return
            task = asyncio.create_task(close())
            self._close_task = task

        cancelled = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                cancelled = True

        task.result()
        if cancelled:
            raise asyncio.CancelledError

    def __repr__(self) -> str:
        """Describe stream lifecycle without exposing the source or event payloads."""
        return f"SSEStream(closed={self._closed})"


def sse_response(events: AsyncIterator[SSEEvent], *, status: int = 200) -> Response:
    """Create a streamed native Orbit response with SSE content and anti-buffering defaults."""
    if isinstance(status, bool) or not isinstance(status, int) or not 200 <= status <= 599:
        raise ValueError("SSE response status must be a final integer from 200 through 599.")
    if status in {204, 205, 304}:
        raise ValueError("SSE responses require a status that permits a response body.")
    return Response(
        status=status,
        headers={
            "content-type": "text/event-stream; charset=utf-8",
            "cache-control": "no-cache",
            "x-content-type-options": "nosniff",
        },
        stream=SSEStream(events),
    )


def _validate_text(
    value: object,
    field_name: str,
    maximum_bytes: int,
    *,
    line_safe: bool = False,
    reject_null: bool = False,
) -> None:
    """Require valid bounded UTF-8 and optional field-value separator exclusions."""
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be text.")
    if line_safe and any(character in value for character in "\r\n"):
        raise ValueError(f"{field_name} must not contain line breaks.")
    if reject_null and "\x00" in value:
        raise ValueError(f"{field_name} must not contain NUL.")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError:
        raise ValueError(f"{field_name} must be valid UTF-8 text.") from None
    if len(encoded) > maximum_bytes:
        raise ValueError(f"{field_name} exceeds its {maximum_bytes}-byte limit.")


__all__ = ["SSEEvent", "SSEStream", "encode_comment", "sse_response"]
