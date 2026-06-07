"""Admin operations — topic, queue, and consumer group management.

Provides a high-level administration API for managing the broker's
resources programmatically.
"""

from __future__ import annotations

import time
from typing import Any, Optional

from .broker import Broker
from .consumer import AssignmentStrategy
from .topic import TopicConfig


class AdminManager:
    """Administrative operations for the broker.

    Wraps broker operations with additional validation, logging,
    and convenience methods.
    """

    def __init__(self, broker: Broker):
        self.broker = broker
        self._operation_log: list[dict[str, Any]] = []

    def _log_operation(self, operation: str, details: dict[str, Any]) -> None:
        self._operation_log.append({
            "operation": operation,
            "details": details,
            "timestamp": time.time(),
        })

    # --- Topic admin ---

    def create_topic(
        self,
        name: str,
        partitions: int = 4,
        retention_ms: int = 0,
        compaction: bool = False,
        max_message_bytes: int = 0,
    ) -> dict[str, Any]:
        """Create a topic with full configuration."""
        config = TopicConfig(
            num_partitions=partitions,
            retention_ms=retention_ms,
            compaction_enabled=compaction,
            max_message_bytes=max_message_bytes,
        )
        topic = self.broker.create_topic(name, config)
        self._log_operation("create_topic", {"name": name, "partitions": partitions})
        return topic.snapshot()

    def delete_topic(self, name: str) -> bool:
        """Delete a topic and clean up subscriptions."""
        deleted = self.broker.delete_topic(name)
        self._log_operation("delete_topic", {"name": name, "deleted": deleted})
        return deleted

    def describe_topic(self, name: str) -> Optional[dict[str, Any]]:
        """Get detailed information about a topic."""
        topic = self.broker.get_topic(name)
        if topic:
            return topic.snapshot()
        return None

    def list_topics(self) -> list[dict[str, Any]]:
        """List all topics with metadata."""
        results = []
        for name in self.broker.list_topics():
            topic = self.broker.get_topic(name)
            if topic:
                results.append({
                    "name": name,
                    "partitions": topic.num_partitions,
                    "messages": topic.total_messages(),
                    "bytes": topic.total_bytes(),
                    "subscribers": topic.subscriber_count,
                })
        return results

    # --- Queue admin ---

    def create_queue(
        self,
        name: str,
        max_size: int = 0,
        visibility_timeout: float = 30.0,
        priority: bool = False,
    ) -> dict[str, Any]:
        """Create a point-to-point queue."""
        q = self.broker.create_queue(name, max_size, visibility_timeout, priority)
        self._log_operation("create_queue", {"name": name, "priority": priority})
        return q.snapshot()

    def delete_queue(self, name: str) -> bool:
        deleted = self.broker.delete_queue(name)
        self._log_operation("delete_queue", {"name": name, "deleted": deleted})
        return deleted

    def describe_queue(self, name: str) -> Optional[dict[str, Any]]:
        q = self.broker.get_queue(name)
        if q:
            return q.snapshot()
        return None

    def list_queues(self) -> list[dict[str, Any]]:
        results = []
        for name in self.broker.list_queues():
            q = self.broker.get_queue(name)
            if q:
                results.append(q.snapshot())
        return results

    def purge_queue(self, name: str) -> int:
        q = self.broker.get_queue(name)
        if q:
            return q.purge()
        return 0

    # --- Consumer group admin ---

    def create_consumer_group(
        self,
        group_id: str,
        strategy: str = "range",
    ) -> dict[str, Any]:
        strategy_enum = AssignmentStrategy(strategy)
        group = self.broker.create_consumer_group(group_id, strategy_enum)
        self._log_operation("create_consumer_group", {"group_id": group_id})
        return group.snapshot()

    def delete_consumer_group(self, group_id: str) -> bool:
        deleted = self.broker.delete_consumer_group(group_id)
        self._log_operation("delete_consumer_group", {
            "group_id": group_id, "deleted": deleted,
        })
        return deleted

    def describe_consumer_group(self, group_id: str) -> Optional[dict[str, Any]]:
        group = self.broker.get_consumer_group(group_id)
        if group:
            return group.snapshot()
        return None

    def list_consumer_groups(self) -> list[dict[str, Any]]:
        results = []
        for gid in self.broker.list_consumer_groups():
            group = self.broker.get_consumer_group(gid)
            if group:
                results.append({
                    "group_id": gid,
                    "members": group.member_count,
                    "total_lag": group.total_lag(),
                })
        return results

    # --- Maintenance ---

    def run_retention(self) -> dict[str, int]:
        """Run retention policy on all topics."""
        result = self.broker.apply_retention()
        self._log_operation("retention", {"removed": result})
        return result

    def run_compaction(self) -> dict[str, int]:
        """Run log compaction on eligible topics."""
        result = self.broker.compact_topics()
        self._log_operation("compaction", {"removed": result})
        return result

    def remove_dead_consumers(self) -> dict[str, list[str]]:
        result = self.broker.remove_dead_consumers()
        self._log_operation("remove_dead_consumers", {"removed": result})
        return result

    # --- Stats ---

    def broker_stats(self) -> dict[str, Any]:
        """Full broker statistics."""
        return self.broker.snapshot()

    def operation_log(self, limit: int = 50) -> list[dict[str, Any]]:
        """Recent admin operations."""
        return self._operation_log[-limit:]

    def __repr__(self) -> str:
        return f"AdminManager(broker={self.broker!r})"
