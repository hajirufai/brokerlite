"""Dead letter queue — handling permanently failed messages.

When a message exceeds its maximum retry attempts or is explicitly
rejected, it's routed to a dead letter queue for inspection, debugging,
and optional replay.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Optional

from .message import Message


@dataclass
class DLQEntry:
    """A dead letter queue entry — a failed message with context.

    Attributes:
        message: The original message that failed.
        reason: Why the message was dead-lettered.
        original_topic: The topic the message was originally sent to.
        attempts: How many delivery attempts were made.
        dead_lettered_at: When the message was moved to the DLQ.
        error_details: Optional additional error information.
    """
    message: Message
    reason: str
    original_topic: str
    attempts: int
    dead_lettered_at: float
    error_details: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "message": self.message.to_dict(),
            "reason": self.reason,
            "original_topic": self.original_topic,
            "attempts": self.attempts,
            "dead_lettered_at": self.dead_lettered_at,
            "error_details": self.error_details,
        }


class DeadLetterQueue:
    """Dead letter queue for permanently failed messages.

    Each topic can have an associated DLQ. Failed messages are stored
    with their failure context for debugging and optional replay.
    """

    def __init__(
        self,
        name: str,
        max_size: int = 10000,
    ):
        self.name = name
        self.max_size = max_size

        self._entries: deque[DLQEntry] = deque(maxlen=max_size if max_size > 0 else None)
        self._lock = threading.RLock()
        self._total_received = 0
        self._total_replayed = 0

    def add(
        self,
        message: Message,
        reason: str,
        attempts: int = 0,
        error_details: Optional[str] = None,
    ) -> DLQEntry:
        """Add a failed message to the DLQ."""
        entry = DLQEntry(
            message=message,
            reason=reason,
            original_topic=message.topic,
            attempts=attempts,
            dead_lettered_at=time.time(),
            error_details=error_details,
        )

        with self._lock:
            self._entries.append(entry)
            self._total_received += 1

        # Tag the message header
        message.headers.set("x-dlq-reason", reason)
        message.headers.set("x-dlq-original-topic", message.topic)
        message.headers.set("x-dlq-attempts", str(attempts))

        return entry

    def peek(self, count: int = 10) -> list[DLQEntry]:
        """Peek at the oldest entries without removing them."""
        with self._lock:
            return list(self._entries)[:count]

    def pop(self) -> Optional[DLQEntry]:
        """Remove and return the oldest entry."""
        with self._lock:
            if self._entries:
                return self._entries.popleft()
            return None

    def replay(self, count: int = 0) -> list[Message]:
        """Remove entries and return their messages for reprocessing.

        Args:
            count: Number of entries to replay. 0 = all.
        """
        with self._lock:
            messages = []
            n = count if count > 0 else len(self._entries)

            for _ in range(min(n, len(self._entries))):
                entry = self._entries.popleft()
                # Reset DLQ headers
                entry.message.headers.remove("x-dlq-reason")
                entry.message.headers.remove("x-dlq-original-topic")
                entry.message.headers.remove("x-dlq-attempts")
                messages.append(entry.message)
                self._total_replayed += 1

            return messages

    def purge(self) -> int:
        """Remove all entries. Returns count removed."""
        with self._lock:
            count = len(self._entries)
            self._entries.clear()
            return count

    def filter_by_reason(self, reason: str) -> list[DLQEntry]:
        """Get entries matching a specific failure reason."""
        with self._lock:
            return [e for e in self._entries if reason in e.reason]

    def filter_by_topic(self, topic: str) -> list[DLQEntry]:
        """Get entries from a specific original topic."""
        with self._lock:
            return [e for e in self._entries if e.original_topic == topic]

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._entries)

    def oldest_entry_age(self) -> float:
        """Age of the oldest entry in seconds. 0 if empty."""
        with self._lock:
            if self._entries:
                return time.time() - self._entries[0].dead_lettered_at
            return 0.0

    def failure_reasons(self) -> dict[str, int]:
        """Count entries by failure reason."""
        with self._lock:
            reasons: dict[str, int] = {}
            for entry in self._entries:
                reasons[entry.reason] = reasons.get(entry.reason, 0) + 1
            return reasons

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "name": self.name,
                "size": len(self._entries),
                "max_size": self.max_size,
                "total_received": self._total_received,
                "total_replayed": self._total_replayed,
                "oldest_age_seconds": self.oldest_entry_age(),
                "failure_reasons": self.failure_reasons(),
            }

    def __len__(self) -> int:
        return self.size

    def __repr__(self) -> str:
        return (
            f"DeadLetterQueue(name={self.name!r}, "
            f"size={self.size}, "
            f"received={self._total_received})"
        )
