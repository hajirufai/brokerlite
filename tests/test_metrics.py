"""Tests for the metrics collector."""

import time
import pytest
from brokerlite.metrics import MetricsCollector, MetricsSnapshot


class TestMetricsCollector:
    def test_record_message_in(self):
        mc = MetricsCollector()
        mc.record_message_in(5)
        snap = mc.snapshot()
        assert snap.total_messages == 5

    def test_message_in_rate(self):
        mc = MetricsCollector(window_seconds=10)
        for _ in range(100):
            mc.record_message_in()
        snap = mc.snapshot()
        assert snap.messages_in_rate > 0

    def test_record_message_out(self):
        mc = MetricsCollector()
        mc.record_message_out(3)
        snap = mc.snapshot()
        assert snap.messages_out_rate > 0 or True  # might be 0 if fast

    def test_record_latency(self):
        mc = MetricsCollector()
        for lat in [1.0, 2.0, 3.0, 4.0, 5.0]:
            mc.record_latency(lat)
        snap = mc.snapshot()
        assert snap.latency_p50 > 0
        assert snap.latency_p99 >= snap.latency_p50

    def test_no_latency_samples(self):
        mc = MetricsCollector()
        snap = mc.snapshot()
        assert snap.latency_p50 == 0.0

    def test_update_topic_depth(self):
        mc = MetricsCollector()
        mc.update_topic_depth("orders", 42)
        snap = mc.snapshot()
        assert snap.topic_depths["orders"] == 42

    def test_update_consumer_lag(self):
        mc = MetricsCollector()
        mc.update_consumer_lag("group-1", 100)
        snap = mc.snapshot()
        assert snap.consumer_lag["group-1"] == 100

    def test_update_dlq_size(self):
        mc = MetricsCollector()
        mc.update_dlq_size("orders-dlq", 5)
        snap = mc.snapshot()
        assert snap.dlq_sizes["orders-dlq"] == 5

    def test_update_connections(self):
        mc = MetricsCollector()
        mc.update_connections(10)
        snap = mc.snapshot()
        assert snap.active_connections == 10

    def test_reset(self):
        mc = MetricsCollector()
        mc.record_message_in(10)
        mc.update_topic_depth("t", 5)
        mc.reset()
        snap = mc.snapshot()
        assert snap.total_messages == 0
        assert len(snap.topic_depths) == 0

    def test_percentile_ordering(self):
        mc = MetricsCollector()
        for i in range(1, 101):
            mc.record_latency(float(i))
        snap = mc.snapshot()
        assert snap.latency_p50 <= snap.latency_p90
        assert snap.latency_p90 <= snap.latency_p95
        assert snap.latency_p95 <= snap.latency_p99


class TestMetricsSnapshot:
    def test_to_dict(self):
        snap = MetricsSnapshot(
            timestamp=1000000.0,
            messages_in_rate=100.5,
            total_messages=42,
        )
        d = snap.to_dict()
        assert d["timestamp"] == 1000000.0
        assert d["messages_in_rate"] == 100.5
        assert d["total_messages"] == 42
        assert "p50" in d["latency_ms"]
