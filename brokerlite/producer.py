"""Producer — message publishing with batching and partitioning.

Producers send messages to topics. They handle partition assignment,
message batching for efficiency, and delivery callbacks.
"""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from .message import Message, MessageBatch, MessageHeaders


@dataclass
class RecordMetadata:
    """Metadata returned after a message is successfully published.

    Attributes:
        topic: The topic the message was sent to.
        partition: The partition number.
        offset: The offset within the partition.
        timestamp: Broker-assigned timestamp.
        message_id: The message's unique ID.
    """
    topic: str
    partition: int
    offset: int
    timestamp: float
    message_id: str


@dataclass
class ProducerConfig:
    """Producer configuration.

    Attributes:
        batch_size: Max messages per batch before flush.
        linger_ms: Max time to wait for batch to fill (milliseconds).
        max_in_flight: Max unacknowledged batches.
        enable_idempotence: Deduplicate messages by producer_id + sequence.
        acks: Required acknowledgments ("none", "leader", "all").
    """
    batch_size: int = 16
    linger_ms: int = 100
    max_in_flight: int = 5
    enable_idempotence: bool = False
    acks: str = "leader"


class Producer:
    """A message producer that sends messages to the broker.

    Features:
    - Partition assignment (key-hash or round-robin)
    - Message batching (by size and time)
    - Idempotent delivery (optional deduplication)
    - Success/error callbacks
    """

    def __init__(
        self,
        producer_id: Optional[str] = None,
        config: Optional[ProducerConfig] = None,
        publish_fn: Optional[Callable[[Message], RecordMetadata]] = None,
    ):
        self.producer_id = producer_id or f"producer-{uuid.uuid4().hex[:8]}"
        self.config = config or ProducerConfig()
        self._publish_fn = publish_fn

        self._batch: list[Message] = []
        self._lock = threading.RLock()
        self._sequence_number = 0
        self._total_sent = 0
        self._total_errors = 0
        self._last_flush = time.time()
        self._closed = False

        self._on_success: Optional[Callable[[RecordMetadata], None]] = None
        self._on_error: Optional[Callable[[Message, Exception], None]] = None

    def on_success(self, callback: Callable[[RecordMetadata], None]) -> None:
        """Register a callback for successful message delivery."""
        self._on_success = callback

    def on_error(self, callback: Callable[[Message, Exception], None]) -> None:
        """Register a callback for failed message delivery."""
        self._on_error = callback

    def send(
        self,
        topic: str,
        value: bytes | str,
        key: Optional[str] = None,
        headers: Optional[dict[str, str]] = None,
        partition: int = -1,
        priority: int = 0,
        ttl: int = 0,
    ) -> Optional[RecordMetadata]:
        """Send a message to a topic.

        If batching is enabled (batch_size > 1), the message is buffered.
        If batch_size is 1 or buffer is full, the message is sent immediately.

        Returns RecordMetadata if sent immediately, None if batched.
        """
        if self._closed:
            raise RuntimeError("Producer is closed")

        msg = Message(
            topic=topic,
            value=value if isinstance(value, bytes) else value.encode("utf-8"),
            key=key,
            headers=MessageHeaders(headers) if headers else MessageHeaders(),
            partition=partition,
            priority=priority,
            ttl=ttl,
            producer_id=self.producer_id,
        )

        if self.config.enable_idempotence:
            with self._lock:
                msg.sequence_number = self._sequence_number
                self._sequence_number += 1

        if self.config.batch_size <= 1:
            return self._do_send(msg)

        with self._lock:
            self._batch.append(msg)
            if len(self._batch) >= self.config.batch_size:
                return self._flush_batch()

            elapsed_ms = (time.time() - self._last_flush) * 1000
            if elapsed_ms >= self.config.linger_ms:
                return self._flush_batch()

        return None

    def flush(self) -> list[RecordMetadata]:
        """Flush all buffered messages immediately."""
        results: list[RecordMetadata] = []
        with self._lock:
            while self._batch:
                result = self._flush_batch()
                if result:
                    results.append(result)
        return results

    def _flush_batch(self) -> Optional[RecordMetadata]:
        """Send all buffered messages. Returns metadata for last message."""
        last_result = None
        batch = list(self._batch)
        self._batch.clear()
        self._last_flush = time.time()

        for msg in batch:
            result = self._do_send(msg)
            if result:
                last_result = result

        return last_result

    def _do_send(self, message: Message) -> Optional[RecordMetadata]:
        """Actually publish a single message."""
        if not self._publish_fn:
            self._total_sent += 1
            return RecordMetadata(
                topic=message.topic,
                partition=message.partition,
                offset=-1,
                timestamp=time.time(),
                message_id=message.id,
            )

        try:
            metadata = self._publish_fn(message)
            self._total_sent += 1
            if self._on_success:
                self._on_success(metadata)
            return metadata
        except Exception as e:
            self._total_errors += 1
            if self._on_error:
                self._on_error(message, e)
            return None

    @property
    def pending_count(self) -> int:
        """Number of messages waiting in the batch buffer."""
        with self._lock:
            return len(self._batch)

    def close(self) -> None:
        """Flush remaining messages and close the producer."""
        self.flush()
        self._closed = True

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "producer_id": self.producer_id,
                "total_sent": self._total_sent,
                "total_errors": self._total_errors,
                "pending_batch": len(self._batch),
                "idempotent": self.config.enable_idempotence,
                "sequence_number": self._sequence_number,
                "closed": self._closed,
            }

    def __repr__(self) -> str:
        return (
            f"Producer(id={self.producer_id!r}, "
            f"sent={self._total_sent}, "
            f"pending={self.pending_count})"
        )
