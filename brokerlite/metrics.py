"""Broker metrics — throughput, latency, queue depth, consumer lag.

Collects and aggregates operational metrics from the broker
for monitoring and dashboarding.
"""

from __future__ import annotations

import statistics
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class MetricsSnapshot:
    """A point-in-time snapshot of broker metrics.

    Attributes:
        timestamp: When the snapshot was taken.
        messages_in_rate: Messages received per second.
        messages_out_rate: Messages delivered per second.
        total_messages: Total messages across all topics.
        total_bytes: Total bytes across all topics.
        topic_depths: Message count per topic.
        consumer_lag: Lag per consumer group.
        active_connections: Number of connected clients.
        dlq_sizes: Dead letter queue sizes.
        latency_p50: 50th percentile produce latency (ms).
        latency_p90: 90th percentile produce latency (ms).
        latency_p95: 95th percentile produce latency (ms).
        latency_p99: 99th percentile produce latency (ms).
    """
    timestamp: float
    messages_in_rate: float = 0.0
    messages_out_rate: float = 0.0
    total_messages: int = 0
    total_bytes: int = 0
    topic_depths: dict[str, int] = field(default_factory=dict)
    consumer_lag: dict[str, int] = field(default_factory=dict)
    active_connections: int = 0
    dlq_sizes: dict[str, int] = field(default_factory=dict)
    latency_p50: float = 0.0
    latency_p90: float = 0.0
    latency_p95: float = 0.0
    latency_p99: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "messages_in_rate": round(self.messages_in_rate, 2),
            "messages_out_rate": round(self.messages_out_rate, 2),
            "total_messages": self.total_messages,
            "total_bytes": self.total_bytes,
            "topic_depths": self.topic_depths,
            "consumer_lag": self.consumer_lag,
            "active_connections": self.active_connections,
            "dlq_sizes": self.dlq_sizes,
            "latency_ms": {
                "p50": round(self.latency_p50, 3),
                "p90": round(self.latency_p90, 3),
                "p95": round(self.latency_p95, 3),
                "p99": round(self.latency_p99, 3),
            },
        }


class MetricsCollector:
    """Collects and aggregates broker metrics.

    Tracks message rates, latencies, queue depths, and consumer lag
    over configurable time windows.
    """

    def __init__(self, window_seconds: int = 60, max_latency_samples: int = 10000):
        self.window_seconds = window_seconds
        self.max_latency_samples = max_latency_samples

        self._messages_in: deque[float] = deque()  # timestamps
        self._messages_out: deque[float] = deque()
        self._latencies: deque[float] = deque(maxlen=max_latency_samples)  # milliseconds
        self._lock = threading.RLock()

        self._topic_depths: dict[str, int] = {}
        self._consumer_lag: dict[str, int] = {}
        self._dlq_sizes: dict[str, int] = {}
        self._active_connections = 0
        self._total_messages = 0
        self._total_bytes = 0

    def record_message_in(self, count: int = 1) -> None:
        """Record incoming message(s)."""
        now = time.time()
        with self._lock:
            for _ in range(count):
                self._messages_in.append(now)
            self._total_messages += count

    def record_message_out(self, count: int = 1) -> None:
        """Record outgoing message(s)."""
        now = time.time()
        with self._lock:
            for _ in range(count):
                self._messages_out.append(now)

    def record_latency(self, latency_ms: float) -> None:
        """Record a produce latency measurement in milliseconds."""
        with self._lock:
            self._latencies.append(latency_ms)

    def update_topic_depth(self, topic: str, depth: int) -> None:
        """Update the current depth of a topic."""
        with self._lock:
            self._topic_depths[topic] = depth

    def update_consumer_lag(self, group_id: str, lag: int) -> None:
        """Update the lag for a consumer group."""
        with self._lock:
            self._consumer_lag[group_id] = lag

    def update_dlq_size(self, name: str, size: int) -> None:
        """Update the size of a dead letter queue."""
        with self._lock:
            self._dlq_sizes[name] = size

    def update_connections(self, count: int) -> None:
        """Update the active connection count."""
        with self._lock:
            self._active_connections = count

    def update_total_bytes(self, total: int) -> None:
        """Update the total bytes across all topics."""
        with self._lock:
            self._total_bytes = total

    def _calculate_rate(self, timestamps: deque[float]) -> float:
        """Calculate messages per second over the window."""
        now = time.time()
        cutoff = now - self.window_seconds

        while timestamps and timestamps[0] < cutoff:
            timestamps.popleft()

        if not timestamps:
            return 0.0

        elapsed = now - timestamps[0] if len(timestamps) > 1 else self.window_seconds
        if elapsed <= 0:
            return 0.0
        return len(timestamps) / elapsed

    def _calculate_percentiles(self) -> tuple[float, float, float, float]:
        """Calculate p50, p90, p95, p99 latency percentiles."""
        if not self._latencies:
            return 0.0, 0.0, 0.0, 0.0

        sorted_latencies = sorted(self._latencies)
        n = len(sorted_latencies)

        def percentile(p: float) -> float:
            idx = int(p / 100.0 * (n - 1))
            return sorted_latencies[min(idx, n - 1)]

        return (
            percentile(50),
            percentile(90),
            percentile(95),
            percentile(99),
        )

    def snapshot(self) -> MetricsSnapshot:
        """Take a point-in-time metrics snapshot."""
        with self._lock:
            in_rate = self._calculate_rate(self._messages_in)
            out_rate = self._calculate_rate(self._messages_out)
            p50, p90, p95, p99 = self._calculate_percentiles()

            return MetricsSnapshot(
                timestamp=time.time(),
                messages_in_rate=in_rate,
                messages_out_rate=out_rate,
                total_messages=self._total_messages,
                total_bytes=self._total_bytes,
                topic_depths=dict(self._topic_depths),
                consumer_lag=dict(self._consumer_lag),
                active_connections=self._active_connections,
                dlq_sizes=dict(self._dlq_sizes),
                latency_p50=p50,
                latency_p90=p90,
                latency_p95=p95,
                latency_p99=p99,
            )

    def reset(self) -> None:
        """Reset all metrics."""
        with self._lock:
            self._messages_in.clear()
            self._messages_out.clear()
            self._latencies.clear()
            self._topic_depths.clear()
            self._consumer_lag.clear()
            self._dlq_sizes.clear()
            self._active_connections = 0
            self._total_messages = 0
            self._total_bytes = 0

    def __repr__(self) -> str:
        snap = self.snapshot()
        return (
            f"MetricsCollector(in={snap.messages_in_rate:.1f}/s, "
            f"out={snap.messages_out_rate:.1f}/s, "
            f"p99={snap.latency_p99:.1f}ms)"
        )
