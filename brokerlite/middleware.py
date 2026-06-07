"""Message middleware — transform, filter, and route messages.

Middleware functions are applied to every message passing through
the broker, enabling cross-cutting concerns like logging, filtering,
deduplication, and routing.
"""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from typing import Callable, Optional

from .message import Message

logger = logging.getLogger(__name__)


class Middleware(ABC):
    """Base class for message middleware.

    Each middleware receives a message and returns either:
    - The message (possibly modified) to continue processing
    - None to drop the message
    """

    @abstractmethod
    def process(self, message: Message) -> Optional[Message]:
        """Process a message. Return None to filter it out."""

    @property
    def name(self) -> str:
        return self.__class__.__name__


class MiddlewarePipeline:
    """Ordered pipeline of middleware processors.

    Messages pass through each middleware in order. If any middleware
    returns None, the message is dropped and subsequent middleware
    is skipped.
    """

    def __init__(self):
        self._middleware: list[Middleware] = []

    def add(self, middleware: Middleware) -> None:
        """Add a middleware to the end of the pipeline."""
        self._middleware.append(middleware)

    def remove(self, name: str) -> bool:
        """Remove a middleware by class name."""
        for i, m in enumerate(self._middleware):
            if m.name == name:
                self._middleware.pop(i)
                return True
        return False

    def process(self, message: Message) -> Optional[Message]:
        """Run a message through the full pipeline.

        Returns the processed message or None if filtered.
        """
        current = message
        for mw in self._middleware:
            current = mw.process(current)
            if current is None:
                return None
        return current

    @property
    def middleware_names(self) -> list[str]:
        return [m.name for m in self._middleware]

    def clear(self) -> None:
        self._middleware.clear()

    def __len__(self) -> int:
        return len(self._middleware)

    def __repr__(self) -> str:
        names = ", ".join(m.name for m in self._middleware)
        return f"MiddlewarePipeline([{names}])"


class LoggingMiddleware(Middleware):
    """Logs every message passing through."""

    def __init__(self, log_level: int = logging.DEBUG):
        self.log_level = log_level
        self.message_count = 0

    def process(self, message: Message) -> Optional[Message]:
        self.message_count += 1
        logger.log(
            self.log_level,
            "Message [%s] topic=%s key=%s size=%d",
            message.id[:8],
            message.topic,
            message.key,
            message.size_bytes,
        )
        return message


class FilterMiddleware(Middleware):
    """Drops messages that match a predicate.

    Messages where the predicate returns True are filtered out.
    """

    def __init__(self, predicate: Callable[[Message], bool]):
        self._predicate = predicate
        self.filtered_count = 0

    def process(self, message: Message) -> Optional[Message]:
        if self._predicate(message):
            self.filtered_count += 1
            return None
        return message


class TransformMiddleware(Middleware):
    """Applies a transformation function to each message."""

    def __init__(self, transform_fn: Callable[[Message], Message]):
        self._transform = transform_fn
        self.transformed_count = 0

    def process(self, message: Message) -> Optional[Message]:
        self.transformed_count += 1
        return self._transform(message)


class DeduplicationMiddleware(Middleware):
    """Drops duplicate messages based on message ID.

    Maintains a set of recently seen message IDs with a configurable
    window size to limit memory usage.
    """

    def __init__(self, window_size: int = 10000):
        self.window_size = window_size
        self._seen: set[str] = set()
        self._order: list[str] = []
        self.duplicate_count = 0

    def process(self, message: Message) -> Optional[Message]:
        if message.id in self._seen:
            self.duplicate_count += 1
            return None

        self._seen.add(message.id)
        self._order.append(message.id)

        while len(self._order) > self.window_size:
            old_id = self._order.pop(0)
            self._seen.discard(old_id)

        return message


class RoutingMiddleware(Middleware):
    """Routes messages to different topics based on content.

    Uses a routing function that returns the target topic name.
    If the function returns None, the message keeps its original topic.
    """

    def __init__(self, route_fn: Callable[[Message], Optional[str]]):
        self._route_fn = route_fn
        self.routed_count = 0

    def process(self, message: Message) -> Optional[Message]:
        target = self._route_fn(message)
        if target is not None:
            message.topic = target
            self.routed_count += 1
        return message


class TimestampMiddleware(Middleware):
    """Adds a processing timestamp header to each message."""

    def process(self, message: Message) -> Optional[Message]:
        message.headers.set("x-processed-at", str(time.time()))
        return message
