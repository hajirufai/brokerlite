"""Consumer and ConsumerGroup — message consumption with partition assignment.

Consumers subscribe to topics and poll for messages. A ConsumerGroup distributes
partitions among its member consumers so each partition is consumed by exactly
one member — enabling parallel consumption of a topic.
"""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

from .message import Message
from .partition import Partition


class AssignmentStrategy(Enum):
    """Partition assignment strategies for consumer groups."""
    RANGE = "range"
    ROUND_ROBIN = "round_robin"
    STICKY = "sticky"


@dataclass
class ConsumerConfig:
    """Consumer configuration.

    Attributes:
        group_id: Consumer group identifier.
        auto_commit: Whether to auto-commit offsets.
        auto_commit_interval: Seconds between auto-commits.
        max_poll_messages: Maximum messages per poll.
        session_timeout: Seconds before consumer is considered dead.
        assignment_strategy: How partitions are assigned to consumers.
    """
    group_id: str = ""
    auto_commit: bool = True
    auto_commit_interval: float = 5.0
    max_poll_messages: int = 100
    session_timeout: float = 30.0
    assignment_strategy: AssignmentStrategy = AssignmentStrategy.RANGE


class Consumer:
    """A message consumer that reads from assigned partitions.

    Consumers are typically part of a ConsumerGroup, which handles
    partition assignment and rebalancing.
    """

    def __init__(
        self,
        consumer_id: Optional[str] = None,
        config: Optional[ConsumerConfig] = None,
    ):
        self.consumer_id = consumer_id or str(uuid.uuid4())[:12]
        self.config = config or ConsumerConfig()

        self._assigned_partitions: list[Partition] = []
        self._offsets: dict[tuple[str, int], int] = {}  # (topic, partition) -> offset
        self._lock = threading.RLock()
        self._last_poll = time.time()
        self._last_commit = time.time()
        self._total_consumed = 0
        self._active = True

    @property
    def assigned_partitions(self) -> list[Partition]:
        with self._lock:
            return list(self._assigned_partitions)

    def assign(self, partitions: list[Partition]) -> None:
        """Assign partitions to this consumer."""
        with self._lock:
            self._assigned_partitions = list(partitions)
            for p in partitions:
                key = (p.topic, p.partition_id)
                if key not in self._offsets:
                    committed = p.get_committed_offset(self.config.group_id)
                    self._offsets[key] = committed

    def revoke(self) -> list[Partition]:
        """Revoke all assigned partitions. Returns the previously assigned list."""
        with self._lock:
            old = self._assigned_partitions
            self._assigned_partitions = []
            return old

    def poll(self, max_messages: Optional[int] = None) -> list[Message]:
        """Poll for messages from assigned partitions.

        Returns up to max_messages from all assigned partitions
        in round-robin fashion.
        """
        limit = max_messages or self.config.max_poll_messages
        messages: list[Message] = []

        with self._lock:
            self._last_poll = time.time()

            if not self._assigned_partitions:
                return messages

            per_partition = max(1, limit // len(self._assigned_partitions))

            for partition in self._assigned_partitions:
                key = (partition.topic, partition.partition_id)
                offset = self._offsets.get(key, 0)
                batch = partition.read(offset, per_partition)
                messages.extend(batch)

                if batch:
                    last_offset = batch[-1].offset
                    self._offsets[key] = last_offset + 1

            self._total_consumed += len(messages)

            if self.config.auto_commit:
                now = time.time()
                if now - self._last_commit >= self.config.auto_commit_interval:
                    self._do_commit()

        return messages[:limit]

    def commit(self) -> dict[tuple[str, int], int]:
        """Manually commit current offsets."""
        with self._lock:
            return self._do_commit()

    def _do_commit(self) -> dict[tuple[str, int], int]:
        """Internal commit — assumes lock is held."""
        committed: dict[tuple[str, int], int] = {}
        for partition in self._assigned_partitions:
            key = (partition.topic, partition.partition_id)
            offset = self._offsets.get(key, 0)
            partition.commit_offset(self.config.group_id, offset)
            committed[key] = offset
        self._last_commit = time.time()
        return committed

    def seek(self, topic: str, partition_id: int, offset: int) -> None:
        """Seek to a specific offset for a partition."""
        with self._lock:
            self._offsets[(topic, partition_id)] = offset

    def seek_to_beginning(self) -> None:
        """Seek all assigned partitions to their earliest offset."""
        with self._lock:
            for p in self._assigned_partitions:
                self._offsets[(p.topic, p.partition_id)] = p.earliest_offset

    def seek_to_end(self) -> None:
        """Seek all assigned partitions to their latest offset."""
        with self._lock:
            for p in self._assigned_partitions:
                self._offsets[(p.topic, p.partition_id)] = p.current_offset

    def position(self, topic: str, partition_id: int) -> int:
        """Get current read position for a partition."""
        with self._lock:
            return self._offsets.get((topic, partition_id), 0)

    @property
    def is_alive(self) -> bool:
        """Check if this consumer is active and has polled within session timeout."""
        if not self._active:
            return False
        return (time.time() - self._last_poll) < self.config.session_timeout

    def close(self) -> None:
        """Close the consumer and commit final offsets."""
        with self._lock:
            self._do_commit()
            self._active = False

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "consumer_id": self.consumer_id,
                "group_id": self.config.group_id,
                "assigned_partitions": [
                    f"{p.topic}-{p.partition_id}"
                    for p in self._assigned_partitions
                ],
                "offsets": {
                    f"{t}-{p}": o for (t, p), o in self._offsets.items()
                },
                "total_consumed": self._total_consumed,
                "active": self._active,
                "last_poll": self._last_poll,
            }

    def __repr__(self) -> str:
        return (
            f"Consumer(id={self.consumer_id!r}, "
            f"partitions={len(self._assigned_partitions)}, "
            f"consumed={self._total_consumed})"
        )


class ConsumerGroup:
    """A group of consumers that share topic partitions.

    Each partition in a subscribed topic is assigned to exactly one consumer
    in the group. When consumers join or leave, partitions are rebalanced.
    """

    def __init__(
        self,
        group_id: str,
        strategy: AssignmentStrategy = AssignmentStrategy.RANGE,
    ):
        self.group_id = group_id
        self.strategy = strategy

        self._members: dict[str, Consumer] = {}  # consumer_id -> Consumer
        self._subscribed_topics: dict[str, list[Partition]] = {}  # topic -> partitions
        self._lock = threading.RLock()
        self._generation = 0
        self._created_at = time.time()

    @property
    def members(self) -> list[Consumer]:
        with self._lock:
            return list(self._members.values())

    @property
    def member_count(self) -> int:
        with self._lock:
            return len(self._members)

    def join(self, consumer: Consumer) -> None:
        """Add a consumer to the group, triggering rebalance."""
        with self._lock:
            consumer.config.group_id = self.group_id
            self._members[consumer.consumer_id] = consumer
            self._rebalance()

    def leave(self, consumer_id: str) -> Optional[Consumer]:
        """Remove a consumer from the group, triggering rebalance."""
        with self._lock:
            consumer = self._members.pop(consumer_id, None)
            if consumer:
                consumer.revoke()
                self._rebalance()
            return consumer

    def subscribe(self, topic_name: str, partitions: list[Partition]) -> None:
        """Subscribe the group to a topic's partitions."""
        with self._lock:
            self._subscribed_topics[topic_name] = partitions
            self._rebalance()

    def unsubscribe(self, topic_name: str) -> None:
        """Unsubscribe from a topic."""
        with self._lock:
            self._subscribed_topics.pop(topic_name, None)
            self._rebalance()

    def _rebalance(self) -> None:
        """Redistribute partitions among current members.

        Revokes all current assignments, then reassigns using the
        configured strategy.
        """
        if not self._members:
            return

        self._generation += 1

        for consumer in self._members.values():
            consumer.revoke()

        all_partitions: list[Partition] = []
        for partitions in self._subscribed_topics.values():
            all_partitions.extend(partitions)

        if not all_partitions:
            return

        members = list(self._members.values())

        if self.strategy == AssignmentStrategy.RANGE:
            self._assign_range(members, all_partitions)
        elif self.strategy == AssignmentStrategy.ROUND_ROBIN:
            self._assign_round_robin(members, all_partitions)
        else:
            self._assign_range(members, all_partitions)

    def _assign_range(
        self, members: list[Consumer], partitions: list[Partition]
    ) -> None:
        """Range assignment: contiguous partition ranges per consumer."""
        n_members = len(members)
        n_partitions = len(partitions)
        partitions_per = n_partitions // n_members
        remainder = n_partitions % n_members

        idx = 0
        for i, consumer in enumerate(members):
            count = partitions_per + (1 if i < remainder else 0)
            assigned = partitions[idx:idx + count]
            consumer.assign(assigned)
            idx += count

    def _assign_round_robin(
        self, members: list[Consumer], partitions: list[Partition]
    ) -> None:
        """Round-robin assignment: interleaved partition assignment."""
        assignment: dict[str, list[Partition]] = {
            c.consumer_id: [] for c in members
        }
        for i, partition in enumerate(partitions):
            consumer = members[i % len(members)]
            assignment[consumer.consumer_id].append(partition)

        for consumer in members:
            consumer.assign(assignment[consumer.consumer_id])

    def remove_dead_members(self) -> list[str]:
        """Remove consumers that haven't polled within session timeout."""
        with self._lock:
            dead = [
                cid for cid, c in self._members.items()
                if not c.is_alive
            ]
            for cid in dead:
                self._members.pop(cid).revoke()
            if dead:
                self._rebalance()
            return dead

    def consumer_lag(self) -> dict[str, int]:
        """Get lag per consumer."""
        with self._lock:
            lag: dict[str, int] = {}
            for consumer in self._members.values():
                total_lag = 0
                for p in consumer.assigned_partitions:
                    total_lag += p.consumer_lag(self.group_id)
                lag[consumer.consumer_id] = total_lag
            return lag

    def total_lag(self) -> int:
        """Total lag across all consumers."""
        return sum(self.consumer_lag().values())

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "group_id": self.group_id,
                "strategy": self.strategy.value,
                "generation": self._generation,
                "member_count": len(self._members),
                "members": [c.snapshot() for c in self._members.values()],
                "subscribed_topics": list(self._subscribed_topics.keys()),
                "total_lag": self.total_lag(),
                "created_at": self._created_at,
            }

    def __repr__(self) -> str:
        return (
            f"ConsumerGroup(id={self.group_id!r}, "
            f"members={len(self._members)}, "
            f"topics={list(self._subscribed_topics.keys())})"
        )
