"""Broker — the central message routing engine.

The broker manages topics, queues, consumer groups, and routes messages
from producers to consumers. It's the heart of BrokerLite.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Optional

from .consumer import Consumer, ConsumerGroup, AssignmentStrategy
from .message import Message
from .producer import Producer, RecordMetadata
from .queue import MessageQueue, PriorityQueue
from .topic import Topic, TopicConfig
from .middleware import MiddlewarePipeline


@dataclass
class BrokerConfig:
    """Broker configuration.

    Attributes:
        default_partitions: Default number of partitions for new topics.
        default_retention_ms: Default message retention (0 = forever).
        max_topics: Maximum number of topics (0 = unlimited).
        max_queues: Maximum number of queues (0 = unlimited).
        enable_metrics: Whether to collect internal metrics.
    """
    default_partitions: int = 4
    default_retention_ms: int = 0
    max_topics: int = 0
    max_queues: int = 0
    enable_metrics: bool = True


class Broker:
    """Central message broker.

    Routes messages from producers to consumers through topics (pub/sub)
    and queues (point-to-point). Manages consumer groups for scalable
    consumption.
    """

    def __init__(self, config: Optional[BrokerConfig] = None):
        self.config = config or BrokerConfig()

        self._topics: dict[str, Topic] = {}
        self._queues: dict[str, MessageQueue] = {}
        self._priority_queues: dict[str, PriorityQueue] = {}
        self._consumer_groups: dict[str, ConsumerGroup] = {}
        self._producers: dict[str, Producer] = {}
        self._middleware = MiddlewarePipeline()
        self._lock = threading.RLock()
        self._running = False
        self._started_at = 0.0

        self._total_messages_in = 0
        self._total_messages_out = 0

    def start(self) -> None:
        """Start the broker."""
        self._running = True
        self._started_at = time.time()

    def stop(self) -> None:
        """Stop the broker and flush all producers."""
        self._running = False
        for producer in self._producers.values():
            producer.close()

    @property
    def is_running(self) -> bool:
        return self._running

    # --- Topic management ---

    def create_topic(
        self,
        name: str,
        config: Optional[TopicConfig] = None,
    ) -> Topic:
        """Create a new topic.

        Raises ValueError if the topic already exists or limits are exceeded.
        """
        with self._lock:
            if name in self._topics:
                raise ValueError(f"Topic {name!r} already exists")
            if self.config.max_topics > 0 and len(self._topics) >= self.config.max_topics:
                raise ValueError(
                    f"Maximum topics ({self.config.max_topics}) reached"
                )

            cfg = config or TopicConfig(
                num_partitions=self.config.default_partitions,
                retention_ms=self.config.default_retention_ms,
            )
            topic = Topic(name, cfg)
            self._topics[name] = topic
            return topic

    def delete_topic(self, name: str) -> bool:
        """Delete a topic. Returns True if it existed."""
        with self._lock:
            topic = self._topics.pop(name, None)
            if topic:
                for group in self._consumer_groups.values():
                    group.unsubscribe(name)
                return True
            return False

    def get_topic(self, name: str) -> Optional[Topic]:
        """Get a topic by name."""
        with self._lock:
            return self._topics.get(name)

    def list_topics(self) -> list[str]:
        """List all topic names."""
        with self._lock:
            return list(self._topics.keys())

    def topic_exists(self, name: str) -> bool:
        with self._lock:
            return name in self._topics

    # --- Queue management ---

    def create_queue(
        self,
        name: str,
        max_size: int = 0,
        visibility_timeout: float = 30.0,
        priority: bool = False,
    ) -> MessageQueue | PriorityQueue:
        """Create a point-to-point queue."""
        with self._lock:
            if name in self._queues or name in self._priority_queues:
                raise ValueError(f"Queue {name!r} already exists")
            if self.config.max_queues > 0:
                total = len(self._queues) + len(self._priority_queues)
                if total >= self.config.max_queues:
                    raise ValueError(
                        f"Maximum queues ({self.config.max_queues}) reached"
                    )

            if priority:
                q = PriorityQueue(name, max_size, visibility_timeout)
                self._priority_queues[name] = q
                return q
            else:
                q = MessageQueue(name, max_size, visibility_timeout)
                self._queues[name] = q
                return q

    def delete_queue(self, name: str) -> bool:
        """Delete a queue."""
        with self._lock:
            if name in self._queues:
                del self._queues[name]
                return True
            if name in self._priority_queues:
                del self._priority_queues[name]
                return True
            return False

    def get_queue(self, name: str) -> Optional[MessageQueue | PriorityQueue]:
        with self._lock:
            q = self._queues.get(name)
            if q is not None:
                return q
            return self._priority_queues.get(name)

    def list_queues(self) -> list[str]:
        with self._lock:
            return list(self._queues.keys()) + list(self._priority_queues.keys())

    # --- Consumer group management ---

    def create_consumer_group(
        self,
        group_id: str,
        strategy: AssignmentStrategy = AssignmentStrategy.RANGE,
    ) -> ConsumerGroup:
        """Create a consumer group."""
        with self._lock:
            if group_id in self._consumer_groups:
                return self._consumer_groups[group_id]
            group = ConsumerGroup(group_id, strategy)
            self._consumer_groups[group_id] = group
            return group

    def get_consumer_group(self, group_id: str) -> Optional[ConsumerGroup]:
        with self._lock:
            return self._consumer_groups.get(group_id)

    def delete_consumer_group(self, group_id: str) -> bool:
        with self._lock:
            return self._consumer_groups.pop(group_id, None) is not None

    def list_consumer_groups(self) -> list[str]:
        with self._lock:
            return list(self._consumer_groups.keys())

    # --- Message publishing ---

    def publish(self, message: Message) -> RecordMetadata:
        """Publish a message to its topic.

        Creates the topic if it doesn't exist.
        Applies middleware pipeline before publishing.
        """
        processed = self._middleware.process(message)
        if processed is None:
            raise ValueError("Message was filtered by middleware")

        with self._lock:
            topic = self._topics.get(processed.topic)
            if topic is None:
                topic = self.create_topic(processed.topic)

            offset = topic.publish(processed)
            self._total_messages_in += 1

            return RecordMetadata(
                topic=processed.topic,
                partition=processed.partition,
                offset=offset,
                timestamp=processed.broker_timestamp,
                message_id=processed.id,
            )

    def enqueue(self, queue_name: str, message: Message) -> None:
        """Enqueue a message to a point-to-point queue."""
        with self._lock:
            q = self._queues.get(queue_name)
            if q is None:
                q = self._priority_queues.get(queue_name)
            if q is None:
                raise ValueError(f"Queue {queue_name!r} does not exist")
            q.enqueue(message)
            self._total_messages_in += 1

    # --- Consumer subscription ---

    def subscribe(
        self,
        group_id: str,
        topic_name: str,
        consumer: Consumer,
    ) -> ConsumerGroup:
        """Subscribe a consumer to a topic via a consumer group.

        Creates the group and topic if they don't exist.
        """
        with self._lock:
            topic = self._topics.get(topic_name)
            if topic is None:
                topic = self.create_topic(topic_name)

            group = self._consumer_groups.get(group_id)
            if group is None:
                group = self.create_consumer_group(group_id)

            group.subscribe(topic_name, topic.partitions)
            topic.subscribe(group_id)
            group.join(consumer)
            return group

    # --- Producer management ---

    def register_producer(self, producer: Producer) -> None:
        """Register a producer with the broker."""
        with self._lock:
            self._producers[producer.producer_id] = producer

    def create_producer(self, **kwargs) -> Producer:
        """Create and register a new producer."""
        producer = Producer(
            publish_fn=self.publish,
            **kwargs,
        )
        self.register_producer(producer)
        return producer

    # --- Middleware ---

    @property
    def middleware(self) -> MiddlewarePipeline:
        return self._middleware

    # --- Maintenance ---

    def apply_retention(self) -> dict[str, int]:
        """Apply retention policies across all topics."""
        results = {}
        with self._lock:
            for name, topic in self._topics.items():
                removed = topic.apply_retention()
                if removed > 0:
                    results[name] = removed
        return results

    def compact_topics(self) -> dict[str, int]:
        """Run log compaction on all eligible topics."""
        results = {}
        with self._lock:
            for name, topic in self._topics.items():
                removed = topic.compact()
                if removed > 0:
                    results[name] = removed
        return results

    def remove_dead_consumers(self) -> dict[str, list[str]]:
        """Remove dead consumers from all groups."""
        results = {}
        with self._lock:
            for gid, group in self._consumer_groups.items():
                dead = group.remove_dead_members()
                if dead:
                    results[gid] = dead
        return results

    # --- Stats ---

    def snapshot(self) -> dict[str, Any]:
        """Full broker state snapshot."""
        with self._lock:
            return {
                "running": self._running,
                "uptime": time.time() - self._started_at if self._started_at else 0,
                "topics": {
                    name: topic.snapshot()
                    for name, topic in self._topics.items()
                },
                "queues": {
                    name: q.snapshot()
                    for name, q in self._queues.items()
                },
                "priority_queues": {
                    name: q.snapshot()
                    for name, q in self._priority_queues.items()
                },
                "consumer_groups": {
                    gid: g.snapshot()
                    for gid, g in self._consumer_groups.items()
                },
                "producers": {
                    pid: p.snapshot()
                    for pid, p in self._producers.items()
                },
                "total_messages_in": self._total_messages_in,
                "total_messages_out": self._total_messages_out,
            }

    def __repr__(self) -> str:
        return (
            f"Broker(topics={len(self._topics)}, "
            f"queues={len(self._queues) + len(self._priority_queues)}, "
            f"groups={len(self._consumer_groups)}, "
            f"messages_in={self._total_messages_in})"
        )
