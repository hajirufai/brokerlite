"""Tests for backpressure and flow control."""

import time
import pytest
from brokerlite.backpressure import (
    RateLimiter, BackpressureManager, BackpressureConfig,
)


class TestRateLimiter:
    def test_acquire_under_limit(self):
        rl = RateLimiter(rate=1000, burst=100)
        assert rl.acquire() is True

    def test_burst_capacity(self):
        rl = RateLimiter(rate=10, burst=5)
        results = [rl.acquire() for _ in range(5)]
        assert all(results)

    def test_exceed_burst(self):
        rl = RateLimiter(rate=10, burst=2)
        rl.acquire()
        rl.acquire()
        assert rl.acquire() is False

    def test_refill_over_time(self):
        rl = RateLimiter(rate=1000, burst=5)
        for _ in range(5):
            rl.acquire()
        time.sleep(0.01)
        assert rl.acquire() is True

    def test_wait_success(self):
        rl = RateLimiter(rate=100, burst=1)
        rl.acquire()  # use the burst
        result = rl.wait(timeout=0.1)
        assert result is True

    def test_available_tokens(self):
        rl = RateLimiter(rate=100, burst=50)
        assert rl.available_tokens > 0

    def test_snapshot(self):
        rl = RateLimiter(rate=100, burst=10)
        rl.acquire()
        snap = rl.snapshot()
        assert snap["rate"] == 100
        assert snap["burst"] == 10
        assert snap["total_allowed"] == 1


class TestBackpressureManager:
    def test_publish_allowed(self):
        mgr = BackpressureManager(BackpressureConfig(
            max_queue_depth=1000,
            producer_rate_limit=10000,
        ))
        allowed, reason = mgr.check_publish_allowed("p1", "orders", 100)
        assert allowed is True

    def test_publish_rejected_depth(self):
        mgr = BackpressureManager(BackpressureConfig(
            max_queue_depth=100,
        ))
        allowed, reason = mgr.check_publish_allowed("p1", "orders", 100)
        assert allowed is False
        assert "depth" in reason

    def test_publish_throttled_disabled(self):
        mgr = BackpressureManager(BackpressureConfig(
            enable_throttling=False,
        ))
        allowed, _ = mgr.check_publish_allowed("p1", "t", 999999)
        assert allowed is True

    def test_detect_slow_consumers(self):
        mgr = BackpressureManager(BackpressureConfig(
            slow_consumer_threshold=100,
        ))
        slow = mgr.detect_slow_consumers({
            "c1": 50,
            "c2": 200,
            "c3": 150,
        })
        assert "c2" in slow
        assert "c3" in slow
        assert "c1" not in slow

    def test_topic_at_capacity(self):
        mgr = BackpressureManager(BackpressureConfig(max_queue_depth=50))
        mgr.update_topic_depth("orders", 50)
        assert mgr.is_topic_at_capacity("orders")

    def test_topic_not_at_capacity(self):
        mgr = BackpressureManager(BackpressureConfig(max_queue_depth=100))
        mgr.update_topic_depth("orders", 10)
        assert not mgr.is_topic_at_capacity("orders")

    def test_remove_producer(self):
        mgr = BackpressureManager()
        mgr.get_producer_limiter("p1")
        mgr.remove_producer("p1")
        snap = mgr.snapshot()
        assert "p1" not in snap["producer_limiters"]

    def test_snapshot(self):
        mgr = BackpressureManager()
        mgr.check_publish_allowed("p1", "t", 0)
        snap = mgr.snapshot()
        assert "config" in snap
        assert "producer_limiters" in snap
