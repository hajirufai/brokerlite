"""Tests for the dead letter queue."""

import pytest
from brokerlite.dlq import DeadLetterQueue, DLQEntry
from brokerlite.message import Message


class TestDeadLetterQueue:
    def test_add_entry(self):
        dlq = DeadLetterQueue("orders-dlq")
        msg = Message(topic="orders", value=b"failed")
        entry = dlq.add(msg, "timeout", attempts=3)
        assert dlq.size == 1
        assert entry.reason == "timeout"
        assert entry.original_topic == "orders"

    def test_headers_tagged(self):
        dlq = DeadLetterQueue("dlq")
        msg = Message(topic="t", value=b"v")
        dlq.add(msg, "error", attempts=2)
        assert msg.headers.get("x-dlq-reason") == "error"
        assert msg.headers.get("x-dlq-attempts") == "2"

    def test_peek(self):
        dlq = DeadLetterQueue("dlq")
        for i in range(5):
            dlq.add(Message(topic="t", value=f"m{i}".encode()), "fail")
        entries = dlq.peek(3)
        assert len(entries) == 3
        assert dlq.size == 5  # not removed

    def test_pop(self):
        dlq = DeadLetterQueue("dlq")
        dlq.add(Message(topic="t", value=b"first"), "err")
        dlq.add(Message(topic="t", value=b"second"), "err")
        entry = dlq.pop()
        assert entry.message.value == b"first"
        assert dlq.size == 1

    def test_pop_empty(self):
        dlq = DeadLetterQueue("dlq")
        assert dlq.pop() is None

    def test_replay(self):
        dlq = DeadLetterQueue("dlq")
        for i in range(3):
            dlq.add(Message(topic="t", value=f"m{i}".encode()), "fail")
        messages = dlq.replay(2)
        assert len(messages) == 2
        assert dlq.size == 1
        # DLQ headers should be removed
        assert messages[0].headers.get("x-dlq-reason") is None

    def test_replay_all(self):
        dlq = DeadLetterQueue("dlq")
        for i in range(3):
            dlq.add(Message(topic="t", value=b"v"), "fail")
        messages = dlq.replay()
        assert len(messages) == 3
        assert dlq.size == 0

    def test_purge(self):
        dlq = DeadLetterQueue("dlq")
        for i in range(5):
            dlq.add(Message(topic="t", value=b"v"), "fail")
        count = dlq.purge()
        assert count == 5
        assert dlq.size == 0

    def test_filter_by_reason(self):
        dlq = DeadLetterQueue("dlq")
        dlq.add(Message(topic="t", value=b"v"), "timeout")
        dlq.add(Message(topic="t", value=b"v"), "parse_error")
        dlq.add(Message(topic="t", value=b"v"), "timeout")
        assert len(dlq.filter_by_reason("timeout")) == 2
        assert len(dlq.filter_by_reason("parse_error")) == 1

    def test_filter_by_topic(self):
        dlq = DeadLetterQueue("dlq")
        dlq.add(Message(topic="orders", value=b"v"), "fail")
        dlq.add(Message(topic="events", value=b"v"), "fail")
        assert len(dlq.filter_by_topic("orders")) == 1

    def test_failure_reasons(self):
        dlq = DeadLetterQueue("dlq")
        dlq.add(Message(topic="t", value=b"v"), "timeout")
        dlq.add(Message(topic="t", value=b"v"), "timeout")
        dlq.add(Message(topic="t", value=b"v"), "error")
        reasons = dlq.failure_reasons()
        assert reasons["timeout"] == 2
        assert reasons["error"] == 1

    def test_max_size(self):
        dlq = DeadLetterQueue("dlq", max_size=3)
        for i in range(5):
            dlq.add(Message(topic="t", value=f"m{i}".encode()), "fail")
        assert dlq.size == 3  # oldest evicted

    def test_snapshot(self):
        dlq = DeadLetterQueue("orders-dlq")
        dlq.add(Message(topic="orders", value=b"v"), "fail")
        snap = dlq.snapshot()
        assert snap["name"] == "orders-dlq"
        assert snap["size"] == 1
        assert snap["total_received"] == 1

    def test_len(self):
        dlq = DeadLetterQueue("dlq")
        dlq.add(Message(topic="t", value=b"v"), "fail")
        assert len(dlq) == 1


class TestDLQEntry:
    def test_to_dict(self):
        msg = Message(topic="orders", value=b"data")
        entry = DLQEntry(
            message=msg,
            reason="timeout",
            original_topic="orders",
            attempts=3,
            dead_lettered_at=1000000.0,
        )
        d = entry.to_dict()
        assert d["reason"] == "timeout"
        assert d["original_topic"] == "orders"
        assert d["attempts"] == 3
