"""Topic — named message category with partitioned storage.

A topic is a logical grouping of related messages. Each topic is divided
into one or more partitions for parallelism. Producers send messages to
a topic, and consumers subscribe to topics.
"""

from __future__ import annotations

import hashlib
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Optional

from .message import Message
from .partition import Partition


@dataclass
class TopicConfig:
    """Configuration for a topic.

    Attributes:
        num_partitions: Number of partitions (default 4).
        retention_ms: How long to retain messages (0 = forever).
        max_message_bytes: Maximum size of a single message (0 = unlimited).
        max_partition_size: Maximum messages per partition (0 = unlimited).
        compaction_enabled: Whether key-based compaction is enabled.
    """

    num_partitions: int = 4
    retention_ms: int = 0
    max_message_bytes: int = 0
    max_partition_size: int = 0
    compaction_enabled: bool = False


class Topic:
    """A named message category backed by partitioned storage.

    Messages are assigned to partitions using:
    - Key-based hashing: messages with the same key go to the same partition
    - Round-robin: messages without a key are distributed evenly
    """

    def __init__(self, name: str, config: Optional[TopicConfig] = None):
        self.name = name
        self.config = config or TopicConfig()
        self._partitions: list[Partition] = []
        self._round_robin_counter = 0
        self._lock = threading.RLock()
        self._created_at = time.time()
        self._subscribers: set[str] = set()  # consumer group IDs

        for i in range(self.config.num_partitions):
            p = Partition(
                topic=name,
                partition_id=i,
                max_size=self.config.max_partition_size,
                retention_ms=self.config.retention_ms,
            )
            self._partitions.append(p)

    @property
    def num_partitions(self) -> int:
        return len(self._partitions)

    def get_partition(self, partition_id: int) -> Partition:
        """Get a specific partition by ID."""
        if partition_id < 0 or partition_id >= len(self._partitions):
            raise ValueError(
                f"Partition {partition_id} does not exist in topic {self.name!r} "
                f"(0..{len(self._partitions) - 1})"
            )
        return self._partitions[partition_id]

    @property
    def partitions(self) -> list[Partition]:
        return list(self._partitions)

    def assign_partition(self, key: Optional[str]) -> int:
        """Determine which partition a message should go to.

        Key-based: consistent hash of the key mod num_partitions.
        No key: round-robin across partitions.
        """
        if key is not None:
            h = int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16)
            return h % len(self._partitions)

        with self._lock:
            partition_id = self._round_robin_counter % len(self._partitions)
            self._round_robin_counter += 1
            return partition_id

    def publish(self, message: Message) -> int:
        """Publish a message to the topic.

        Assigns a partition (if not already set) and appends to the partition log.
        Returns the assigned offset.
        """
        if self.config.max_message_bytes > 0:
            if message.size_bytes > self.config.max_message_bytes:
                raise ValueError(
                    f"Message size {message.size_bytes} exceeds max "
                    f"{self.config.max_message_bytes} bytes for topic {self.name!r}"
                )

        if message.partition < 0:
            message.partition = self.assign_partition(message.key)

        partition = self._partitions[message.partition]
        message.topic = self.name
        return partition.append(message)

    def subscribe(self, group_id: str) -> None:
        """Register a consumer group as a subscriber."""
        with self._lock:
            self._subscribers.add(group_id)

    def unsubscribe(self, group_id: str) -> None:
        """Unregister a consumer group."""
        with self._lock:
            self._subscribers.discard(group_id)

    @property
    def subscriber_count(self) -> int:
        with self._lock:
            return len(self._subscribers)

    @property
    def subscribers(self) -> set[str]:
        with self._lock:
            return set(self._subscribers)

    def total_messages(self) -> int:
        """Total messages across all partitions."""
        return sum(p.size for p in self._partitions)

    def total_bytes(self) -> int:
        """Total size across all partitions in bytes."""
        return sum(p.size_bytes for p in self._partitions)

    def apply_retention(self) -> int:
        """Apply retention policy across all partitions."""
        total_removed = 0
        for p in self._partitions:
            total_removed += p.apply_retention()
        return total_removed

    def compact(self) -> int:
        """Run log compaction across all partitions."""
        if not self.config.compaction_enabled:
            return 0
        total_removed = 0
        for p in self._partitions:
            total_removed += p.compact()
        return total_removed

    def snapshot(self) -> dict[str, Any]:
        """Return topic metadata snapshot."""
        return {
            "name": self.name,
            "num_partitions": len(self._partitions),
            "config": {
                "retention_ms": self.config.retention_ms,
                "max_message_bytes": self.config.max_message_bytes,
                "compaction_enabled": self.config.compaction_enabled,
            },
            "total_messages": self.total_messages(),
            "total_bytes": self.total_bytes(),
            "subscribers": list(self._subscribers),
            "partitions": [p.snapshot() for p in self._partitions],
            "created_at": self._created_at,
        }

    def __repr__(self) -> str:
        return (
            f"Topic(name={self.name!r}, partitions={len(self._partitions)}, "
            f"messages={self.total_messages()})"
        )
