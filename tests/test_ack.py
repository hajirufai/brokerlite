"""Tests for the acknowledgment system."""

import time
import pytest
from brokerlite.ack import AckManager, AckMode, PendingAck
from brokerlite.message import Message


class TestAckManager:
    def test_track_and_ack(self):
        mgr = AckManager()
        msg = Message(topic="t", value=b"v")
        mgr.track(msg, "consumer-1")
        assert mgr.pending_count == 1
        assert mgr.acknowledge(msg.id)
        assert mgr.pending_count == 0

    def test_ack_unknown_returns_false(self):
        mgr = AckManager()
        assert not mgr.acknowledge("nonexistent")

    def test_nack_redelivery(self):
        mgr = AckManager(max_attempts=3)
        msg = Message(topic="t", value=b"v")
        mgr.track(msg, "c1")
        result = mgr.negative_acknowledge(msg.id)
        assert result is not None  # should return message for requeue

    def test_nack_max_attempts_dead_letter(self):
        dead = []
        mgr = AckManager(
            max_attempts=1,
            on_dead_letter=lambda m, r: dead.append((m, r)),
        )
        msg = Message(topic="t", value=b"v")
        mgr.track(msg, "c1")
        result = mgr.negative_acknowledge(msg.id)
        assert result is None  # should be dead-lettered
        assert len(dead) == 1

    def test_timeout_redelivery(self):
        mgr = AckManager(ack_timeout=0.01, max_attempts=5)
        msg = Message(topic="t", value=b"v")
        mgr.track(msg, "c1", timeout=0.01)
        time.sleep(0.02)
        timed_out = mgr.check_timeouts()
        assert len(timed_out) == 1
        assert timed_out[0].id == msg.id

    def test_timeout_dead_letter_after_max_attempts(self):
        dead = []
        mgr = AckManager(
            ack_timeout=0.01,
            max_attempts=1,
            on_dead_letter=lambda m, r: dead.append((m, r)),
        )
        msg = Message(topic="t", value=b"v")
        mgr.track(msg, "c1", timeout=0.01)
        time.sleep(0.02)
        timed_out = mgr.check_timeouts()
        assert len(timed_out) == 0  # should be dead-lettered, not redelivered
        assert len(dead) == 1

    def test_pending_for_consumer(self):
        mgr = AckManager()
        m1 = Message(topic="t", value=b"1")
        m2 = Message(topic="t", value=b"2")
        mgr.track(m1, "c1")
        mgr.track(m2, "c2")
        c1_pending = mgr.pending_for_consumer("c1")
        assert len(c1_pending) == 1
        assert c1_pending[0].consumer_id == "c1"

    def test_snapshot(self):
        mgr = AckManager(ack_timeout=10, max_attempts=3)
        msg = Message(topic="t", value=b"v")
        mgr.track(msg, "c1")
        mgr.acknowledge(msg.id)
        snap = mgr.snapshot()
        assert snap["total_acked"] == 1
        assert snap["pending_count"] == 0


class TestAckMode:
    def test_values(self):
        assert AckMode.AT_MOST_ONCE.value == "at_most_once"
        assert AckMode.AT_LEAST_ONCE.value == "at_least_once"
        assert AckMode.EXACTLY_ONCE.value == "exactly_once"
