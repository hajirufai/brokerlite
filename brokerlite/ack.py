"""Acknowledgment system — delivery guarantees for messages.

Supports three delivery semantics:
- At-most-once: fire and forget, no retry
- At-least-once: ack after processing, redeliver on timeout
- Exactly-once: idempotent producer + transactional offset commit

Also handles explicit negative acknowledgment (nack) for
requeue or dead-letter routing.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

from .message import Message


class AckMode(Enum):
    """Message delivery guarantee level."""
    AT_MOST_ONCE = "at_most_once"
    AT_LEAST_ONCE = "at_least_once"
    EXACTLY_ONCE = "exactly_once"


@dataclass
class PendingAck:
    """A message awaiting acknowledgment.

    Tracks the message, when it was sent to the consumer,
    how many delivery attempts have been made, and the deadline
    for acknowledgment.
    """
    message: Message
    consumer_id: str
    sent_at: float
    deadline: float
    attempts: int = 1
    max_attempts: int = 5


class AckManager:
    """Manages message acknowledgments and redelivery.

    Tracks which messages are pending acknowledgment, handles
    timeouts for automatic redelivery, and routes permanently
    failed messages to a dead letter handler.
    """

    def __init__(
        self,
        ack_timeout: float = 30.0,
        max_attempts: int = 5,
        on_dead_letter: Optional[Callable[[Message, str], None]] = None,
    ):
        self.ack_timeout = ack_timeout
        self.max_attempts = max_attempts
        self.on_dead_letter = on_dead_letter

        self._pending: dict[str, PendingAck] = {}  # message_id -> PendingAck
        self._lock = threading.RLock()
        self._total_acked = 0
        self._total_nacked = 0
        self._total_dead_lettered = 0
        self._total_redelivered = 0

    def track(
        self,
        message: Message,
        consumer_id: str,
        timeout: Optional[float] = None,
    ) -> None:
        """Start tracking a message for acknowledgment.

        The message must be acknowledged within the timeout or it will
        be eligible for redelivery.
        """
        t = timeout or self.ack_timeout
        with self._lock:
            existing = self._pending.get(message.id)
            attempts = existing.attempts + 1 if existing else 1

            self._pending[message.id] = PendingAck(
                message=message,
                consumer_id=consumer_id,
                sent_at=time.time(),
                deadline=time.time() + t,
                attempts=attempts,
                max_attempts=self.max_attempts,
            )

    def acknowledge(self, message_id: str) -> bool:
        """Acknowledge successful processing.

        Returns True if the message was pending and is now acknowledged.
        """
        with self._lock:
            if message_id in self._pending:
                del self._pending[message_id]
                self._total_acked += 1
                return True
            return False

    def negative_acknowledge(self, message_id: str) -> Optional[Message]:
        """Negatively acknowledge a message — request redelivery.

        Returns the message for requeue, or None if max attempts exceeded
        (in which case it's routed to the dead letter handler).
        """
        with self._lock:
            pending = self._pending.pop(message_id, None)
            if pending is None:
                return None

            self._total_nacked += 1

            if pending.attempts >= pending.max_attempts:
                self._total_dead_lettered += 1
                if self.on_dead_letter:
                    reason = (
                        f"Max attempts ({pending.max_attempts}) exceeded "
                        f"after {pending.attempts} deliveries"
                    )
                    self.on_dead_letter(pending.message, reason)
                return None

            self._total_redelivered += 1
            return pending.message

    def check_timeouts(self) -> list[Message]:
        """Check for messages that have exceeded their ack timeout.

        Returns messages that need to be redelivered or dead-lettered.
        """
        now = time.time()
        timed_out: list[Message] = []

        with self._lock:
            expired_ids = [
                mid for mid, p in self._pending.items()
                if now >= p.deadline
            ]

            for mid in expired_ids:
                pending = self._pending.pop(mid)

                if pending.attempts >= pending.max_attempts:
                    self._total_dead_lettered += 1
                    if self.on_dead_letter:
                        reason = (
                            f"Ack timeout after {pending.attempts} attempts "
                            f"(timeout={self.ack_timeout}s)"
                        )
                        self.on_dead_letter(pending.message, reason)
                else:
                    self._total_redelivered += 1
                    timed_out.append(pending.message)

        return timed_out

    @property
    def pending_count(self) -> int:
        with self._lock:
            return len(self._pending)

    def get_pending(self, message_id: str) -> Optional[PendingAck]:
        with self._lock:
            return self._pending.get(message_id)

    def pending_for_consumer(self, consumer_id: str) -> list[PendingAck]:
        """Get all pending acks for a specific consumer."""
        with self._lock:
            return [
                p for p in self._pending.values()
                if p.consumer_id == consumer_id
            ]

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "pending_count": len(self._pending),
                "total_acked": self._total_acked,
                "total_nacked": self._total_nacked,
                "total_dead_lettered": self._total_dead_lettered,
                "total_redelivered": self._total_redelivered,
                "ack_timeout": self.ack_timeout,
                "max_attempts": self.max_attempts,
            }

    def __repr__(self) -> str:
        return (
            f"AckManager(pending={self.pending_count}, "
            f"acked={self._total_acked}, "
            f"dead_lettered={self._total_dead_lettered})"
        )
