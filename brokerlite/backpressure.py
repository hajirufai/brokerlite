"""Backpressure and flow control for the message broker.

Prevents producers from overwhelming the broker or consumers by
implementing rate limiting (token bucket), queue depth limits,
and producer throttling.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Optional


class RateLimiter:
    """Token bucket rate limiter.

    Allows a maximum of `rate` operations per second with burst capacity.
    Tokens are replenished at a steady rate.
    """

    def __init__(self, rate: float, burst: int = 0):
        """
        Args:
            rate: Maximum sustained operations per second.
            burst: Maximum burst size (defaults to rate).
        """
        self.rate = rate
        self.burst = burst if burst > 0 else int(rate)
        self._tokens = float(self.burst)
        self._last_refill = time.monotonic()
        self._lock = threading.RLock()
        self._total_allowed = 0
        self._total_rejected = 0

    def acquire(self, tokens: int = 1) -> bool:
        """Try to acquire tokens.

        Returns True if the request is allowed, False if rate-limited.
        """
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                self._total_allowed += 1
                return True
            self._total_rejected += 1
            return False

    def wait(self, tokens: int = 1, timeout: float = 10.0) -> bool:
        """Wait until tokens are available or timeout.

        Returns True if tokens were acquired, False if timed out.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.acquire(tokens):
                return True
            sleep_time = min(tokens / max(self.rate, 0.001), 0.1)
            time.sleep(sleep_time)
        return False

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(
            self.burst,
            self._tokens + elapsed * self.rate,
        )
        self._last_refill = now

    @property
    def available_tokens(self) -> float:
        with self._lock:
            self._refill()
            return self._tokens

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            self._refill()
            return {
                "rate": self.rate,
                "burst": self.burst,
                "available_tokens": round(self._tokens, 2),
                "total_allowed": self._total_allowed,
                "total_rejected": self._total_rejected,
            }

    def __repr__(self) -> str:
        return (
            f"RateLimiter(rate={self.rate}/s, burst={self.burst}, "
            f"tokens={self.available_tokens:.1f})"
        )


@dataclass
class BackpressureConfig:
    """Configuration for backpressure management.

    Attributes:
        max_queue_depth: Maximum messages in a topic before rejecting.
        producer_rate_limit: Max messages/sec per producer.
        slow_consumer_threshold: Lag above which a consumer is considered slow.
        enable_throttling: Whether to throttle producers.
    """
    max_queue_depth: int = 100_000
    producer_rate_limit: float = 10_000.0
    slow_consumer_threshold: int = 1000
    enable_throttling: bool = True


class BackpressureManager:
    """Manages backpressure across the broker.

    Monitors queue depths, consumer lag, and producer rates to
    prevent the system from being overwhelmed.
    """

    def __init__(self, config: Optional[BackpressureConfig] = None):
        self.config = config or BackpressureConfig()
        self._producer_limiters: dict[str, RateLimiter] = {}
        self._topic_depths: dict[str, int] = {}
        self._lock = threading.RLock()
        self._total_throttled = 0
        self._total_rejected = 0

    def get_producer_limiter(self, producer_id: str) -> RateLimiter:
        """Get or create a rate limiter for a producer."""
        with self._lock:
            if producer_id not in self._producer_limiters:
                self._producer_limiters[producer_id] = RateLimiter(
                    rate=self.config.producer_rate_limit,
                    burst=int(self.config.producer_rate_limit * 2),
                )
            return self._producer_limiters[producer_id]

    def check_publish_allowed(
        self,
        producer_id: str,
        topic: str,
        current_depth: int,
    ) -> tuple[bool, str]:
        """Check if a producer is allowed to publish.

        Returns (allowed, reason).
        """
        if not self.config.enable_throttling:
            return True, "Throttling disabled"

        if current_depth >= self.config.max_queue_depth:
            with self._lock:
                self._total_rejected += 1
            return False, (
                f"Topic {topic!r} depth ({current_depth}) exceeds "
                f"limit ({self.config.max_queue_depth})"
            )

        limiter = self.get_producer_limiter(producer_id)
        if not limiter.acquire():
            with self._lock:
                self._total_throttled += 1
            return False, (
                f"Producer {producer_id!r} rate-limited "
                f"({self.config.producer_rate_limit}/s)"
            )

        return True, "OK"

    def update_topic_depth(self, topic: str, depth: int) -> None:
        """Update the tracked depth for a topic."""
        with self._lock:
            self._topic_depths[topic] = depth

    def is_topic_at_capacity(self, topic: str) -> bool:
        """Check if a topic is at or above its depth limit."""
        with self._lock:
            depth = self._topic_depths.get(topic, 0)
            return depth >= self.config.max_queue_depth

    def detect_slow_consumers(
        self, consumer_lag: dict[str, int]
    ) -> list[str]:
        """Identify consumers with lag above the threshold."""
        return [
            consumer_id
            for consumer_id, lag in consumer_lag.items()
            if lag >= self.config.slow_consumer_threshold
        ]

    def remove_producer(self, producer_id: str) -> None:
        """Remove rate limiter for a producer."""
        with self._lock:
            self._producer_limiters.pop(producer_id, None)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "config": {
                    "max_queue_depth": self.config.max_queue_depth,
                    "producer_rate_limit": self.config.producer_rate_limit,
                    "slow_consumer_threshold": self.config.slow_consumer_threshold,
                    "throttling_enabled": self.config.enable_throttling,
                },
                "producer_limiters": {
                    pid: limiter.snapshot()
                    for pid, limiter in self._producer_limiters.items()
                },
                "topic_depths": dict(self._topic_depths),
                "total_throttled": self._total_throttled,
                "total_rejected": self._total_rejected,
            }

    def __repr__(self) -> str:
        return (
            f"BackpressureManager(producers={len(self._producer_limiters)}, "
            f"throttled={self._total_throttled}, "
            f"rejected={self._total_rejected})"
        )
