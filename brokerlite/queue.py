"""Point-to-point message queues.

Unlike pub/sub topics, a queue delivers each message to exactly one consumer.
Supports FIFO ordering, priority-based ordering, bounded capacity, and
visibility timeouts (message is hidden while being processed).
"""

from __future__ import annotations

import heapq
import threading
import time
from collections import deque
from typing import Optional

from .message import Message


class MessageQueue:
    """FIFO point-to-point message queue.

    Each message is delivered to exactly one consumer.
    Supports visibility timeout: a message being processed is hidden
    from other consumers until acknowledged or timed out.
    """

    def __init__(
        self,
        name: str,
        max_size: int = 0,
        visibility_timeout: float = 30.0,
    ):
        self.name = name
        self.max_size = max_size  # 0 = unlimited
        self.visibility_timeout = visibility_timeout

        self._queue: deque[Message] = deque()
        self._in_flight: dict[str, tuple[Message, float]] = {}  # msg_id -> (msg, expiry)
        self._lock = threading.RLock()
        self._total_enqueued = 0
        self._total_dequeued = 0
        self._total_acked = 0
        self._created_at = time.time()

    @property
    def depth(self) -> int:
        """Number of messages available for consumption."""
        with self._lock:
            return len(self._queue)

    @property
    def in_flight_count(self) -> int:
        """Number of messages currently being processed."""
        with self._lock:
            return len(self._in_flight)

    def enqueue(self, message: Message) -> None:
        """Add a message to the end of the queue.

        Raises ValueError if the queue is full.
        """
        with self._lock:
            if self.max_size > 0 and len(self._queue) >= self.max_size:
                raise ValueError(
                    f"Queue {self.name!r} is full (max_size={self.max_size})"
                )
            message.broker_timestamp = time.time()
            self._queue.append(message)
            self._total_enqueued += 1

    def dequeue(self, visibility_timeout: Optional[float] = None) -> Optional[Message]:
        """Remove and return the next message from the queue.

        The message enters in-flight state — it's hidden from other consumers
        until acknowledged or the visibility timeout expires.
        Returns None if the queue is empty.
        """
        self._requeue_timed_out()

        timeout = visibility_timeout or self.visibility_timeout
        with self._lock:
            if not self._queue:
                return None

            message = self._queue.popleft()
            expiry = time.time() + timeout
            self._in_flight[message.id] = (message, expiry)
            self._total_dequeued += 1
            return message

    def acknowledge(self, message_id: str) -> bool:
        """Acknowledge successful processing of a message.

        Removes the message from in-flight state.
        Returns True if the message was found and acknowledged.
        """
        with self._lock:
            if message_id in self._in_flight:
                del self._in_flight[message_id]
                self._total_acked += 1
                return True
            return False

    def nack(self, message_id: str) -> bool:
        """Negative acknowledge — return message to the queue.

        The message goes back to the front of the queue for redelivery.
        Returns True if the message was found and requeued.
        """
        with self._lock:
            entry = self._in_flight.pop(message_id, None)
            if entry:
                msg, _ = entry
                self._queue.appendleft(msg)
                return True
            return False

    def _requeue_timed_out(self) -> int:
        """Requeue messages whose visibility timeout has expired."""
        now = time.time()
        requeued = 0
        with self._lock:
            expired_ids = [
                mid for mid, (_, expiry) in self._in_flight.items()
                if now >= expiry
            ]
            for mid in expired_ids:
                msg, _ = self._in_flight.pop(mid)
                self._queue.appendleft(msg)
                requeued += 1
        return requeued

    def peek(self) -> Optional[Message]:
        """Look at the next message without removing it."""
        with self._lock:
            if self._queue:
                return self._queue[0]
            return None

    def purge(self) -> int:
        """Remove all messages from the queue. Returns count removed."""
        with self._lock:
            count = len(self._queue)
            self._queue.clear()
            return count

    def snapshot(self) -> dict:
        """Return queue metadata snapshot."""
        with self._lock:
            return {
                "name": self.name,
                "depth": len(self._queue),
                "in_flight": len(self._in_flight),
                "total_enqueued": self._total_enqueued,
                "total_dequeued": self._total_dequeued,
                "total_acked": self._total_acked,
                "max_size": self.max_size,
                "visibility_timeout": self.visibility_timeout,
                "created_at": self._created_at,
            }

    def __len__(self) -> int:
        return self.depth

    def __repr__(self) -> str:
        return (
            f"MessageQueue(name={self.name!r}, depth={self.depth}, "
            f"in_flight={self.in_flight_count})"
        )


class PriorityQueue:
    """Priority-based message queue.

    Messages with higher priority (0=lowest, 9=highest) are dequeued first.
    Within the same priority, FIFO ordering is maintained.
    """

    def __init__(
        self,
        name: str,
        max_size: int = 0,
        visibility_timeout: float = 30.0,
    ):
        self.name = name
        self.max_size = max_size
        self.visibility_timeout = visibility_timeout

        # Heap entries: (-priority, sequence, message)
        self._heap: list[tuple[int, int, Message]] = []
        self._sequence = 0
        self._in_flight: dict[str, tuple[Message, float]] = {}
        self._lock = threading.RLock()
        self._total_enqueued = 0
        self._total_dequeued = 0

    @property
    def depth(self) -> int:
        with self._lock:
            return len(self._heap)

    def enqueue(self, message: Message) -> None:
        """Add a message with priority ordering."""
        with self._lock:
            if self.max_size > 0 and len(self._heap) >= self.max_size:
                raise ValueError(
                    f"PriorityQueue {self.name!r} is full (max_size={self.max_size})"
                )
            message.broker_timestamp = time.time()
            entry = (-message.priority, self._sequence, message)
            heapq.heappush(self._heap, entry)
            self._sequence += 1
            self._total_enqueued += 1

    def dequeue(self, visibility_timeout: Optional[float] = None) -> Optional[Message]:
        """Remove and return the highest-priority message."""
        self._requeue_timed_out()
        timeout = visibility_timeout or self.visibility_timeout

        with self._lock:
            if not self._heap:
                return None

            _, _, message = heapq.heappop(self._heap)
            expiry = time.time() + timeout
            self._in_flight[message.id] = (message, expiry)
            self._total_dequeued += 1
            return message

    def acknowledge(self, message_id: str) -> bool:
        with self._lock:
            if message_id in self._in_flight:
                del self._in_flight[message_id]
                return True
            return False

    def nack(self, message_id: str) -> bool:
        with self._lock:
            entry = self._in_flight.pop(message_id, None)
            if entry:
                msg, _ = entry
                heap_entry = (-msg.priority, self._sequence, msg)
                heapq.heappush(self._heap, heap_entry)
                self._sequence += 1
                return True
            return False

    def _requeue_timed_out(self) -> int:
        now = time.time()
        requeued = 0
        with self._lock:
            expired_ids = [
                mid for mid, (_, expiry) in self._in_flight.items()
                if now >= expiry
            ]
            for mid in expired_ids:
                msg, _ = self._in_flight.pop(mid)
                heap_entry = (-msg.priority, self._sequence, msg)
                heapq.heappush(self._heap, heap_entry)
                self._sequence += 1
                requeued += 1
        return requeued

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "name": self.name,
                "depth": len(self._heap),
                "in_flight": len(self._in_flight),
                "total_enqueued": self._total_enqueued,
                "total_dequeued": self._total_dequeued,
                "max_size": self.max_size,
            }

    def __len__(self) -> int:
        return self.depth

    def __repr__(self) -> str:
        return f"PriorityQueue(name={self.name!r}, depth={self.depth})"
