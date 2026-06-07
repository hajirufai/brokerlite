"""Partition — ordered, append-only log of messages.

Each topic is divided into partitions. A partition is an ordered, immutable
sequence of messages that is continually appended to. Each message within
a partition is assigned a sequential offset.
"""

from __future__ import annotations

import threading
import time
from typing import Optional

from .message import Message


class Partition:
    """An ordered, append-only message log.

    A partition maintains:
    - An ordered list of messages
    - A monotonically increasing write offset
    - Per-consumer-group committed offsets
    - Retention policies (time-based, size-based)

    Thread-safe via a reentrant lock.
    """

    def __init__(
        self,
        topic: str,
        partition_id: int,
        max_size: int = 0,
        retention_ms: int = 0,
    ):
        self.topic = topic
        self.partition_id = partition_id
        self.max_size = max_size  # 0 = unlimited
        self.retention_ms = retention_ms  # 0 = forever

        self._log: list[Message] = []
        self._next_offset: int = 0
        self._committed_offsets: dict[str, int] = {}  # group_id -> offset
        self._lock = threading.RLock()
        self._created_at = time.time()

    @property
    def current_offset(self) -> int:
        """The next offset that will be assigned."""
        with self._lock:
            return self._next_offset

    @property
    def earliest_offset(self) -> int:
        """Earliest available offset (may differ from 0 after retention)."""
        with self._lock:
            if not self._log:
                return self._next_offset
            return self._log[0].offset

    @property
    def size(self) -> int:
        """Number of messages currently in the partition."""
        with self._lock:
            return len(self._log)

    @property
    def size_bytes(self) -> int:
        """Total size of all messages in bytes."""
        with self._lock:
            return sum(m.size_bytes for m in self._log)

    def append(self, message: Message) -> int:
        """Append a message to the partition log.

        Returns the assigned offset.
        Raises ValueError if max_size is exceeded.
        """
        with self._lock:
            if self.max_size > 0 and len(self._log) >= self.max_size:
                raise ValueError(
                    f"Partition {self.topic}-{self.partition_id} is full "
                    f"(max_size={self.max_size})"
                )

            offset = self._next_offset
            message.partition = self.partition_id
            message.offset = offset
            message.broker_timestamp = time.time()
            self._log.append(message)
            self._next_offset += 1
            return offset

    def read(
        self,
        offset: int,
        max_messages: int = 100,
    ) -> list[Message]:
        """Read messages starting from the given offset.

        Returns up to max_messages starting at offset.
        """
        with self._lock:
            if not self._log:
                return []

            base_offset = self._log[0].offset
            start_idx = offset - base_offset
            if start_idx < 0:
                start_idx = 0
            if start_idx >= len(self._log):
                return []

            end_idx = min(start_idx + max_messages, len(self._log))
            return list(self._log[start_idx:end_idx])

    def read_at(self, offset: int) -> Optional[Message]:
        """Read a single message at the exact offset."""
        with self._lock:
            if not self._log:
                return None
            base_offset = self._log[0].offset
            idx = offset - base_offset
            if 0 <= idx < len(self._log):
                return self._log[idx]
            return None

    def commit_offset(self, group_id: str, offset: int) -> None:
        """Commit the consumer group offset (the next offset to read)."""
        with self._lock:
            self._committed_offsets[group_id] = offset

    def get_committed_offset(self, group_id: str) -> int:
        """Get the committed offset for a consumer group.

        Returns 0 if no offset has been committed.
        """
        with self._lock:
            return self._committed_offsets.get(group_id, 0)

    def consumer_lag(self, group_id: str) -> int:
        """Calculate lag: how far behind the consumer group is."""
        with self._lock:
            committed = self._committed_offsets.get(group_id, 0)
            return max(0, self._next_offset - committed)

    def apply_retention(self) -> int:
        """Remove messages older than retention_ms.

        Returns the number of messages removed.
        """
        if self.retention_ms <= 0:
            return 0

        cutoff = time.time() - (self.retention_ms / 1000.0)
        with self._lock:
            original_len = len(self._log)
            self._log = [m for m in self._log if m.broker_timestamp >= cutoff]
            return original_len - len(self._log)

    def compact(self) -> int:
        """Key-based log compaction: keep only the latest message per key.

        Messages without a key are always retained.
        Returns the number of messages removed.
        """
        with self._lock:
            original_len = len(self._log)
            latest_by_key: dict[str, int] = {}
            for i, msg in enumerate(self._log):
                if msg.key is not None:
                    latest_by_key[msg.key] = i

            compacted = []
            for i, msg in enumerate(self._log):
                if msg.key is None:
                    compacted.append(msg)
                elif latest_by_key.get(msg.key) == i:
                    compacted.append(msg)

            self._log = compacted
            return original_len - len(self._log)

    def clear(self) -> None:
        """Clear all messages from the partition."""
        with self._lock:
            self._log.clear()

    def snapshot(self) -> dict:
        """Return partition metadata snapshot."""
        with self._lock:
            return {
                "topic": self.topic,
                "partition_id": self.partition_id,
                "current_offset": self._next_offset,
                "earliest_offset": self.earliest_offset,
                "message_count": len(self._log),
                "size_bytes": self.size_bytes,
                "committed_offsets": dict(self._committed_offsets),
            }

    def __repr__(self) -> str:
        return (
            f"Partition(topic={self.topic!r}, id={self.partition_id}, "
            f"messages={self.size}, offset={self.current_offset})"
        )
