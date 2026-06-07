"""Tests for MessageQueue and PriorityQueue."""

import pytest
from brokerlite.queue import MessageQueue, PriorityQueue
from brokerlite.message import Message


class TestMessageQueue:
    def test_enqueue_dequeue(self):
        q = MessageQueue("tasks")
        msg = Message(topic="tasks", value=b"task-1")
        q.enqueue(msg)
        result = q.dequeue()
        assert result is not None
        assert result.value == b"task-1"

    def test_fifo_order(self):
        q = MessageQueue("tasks")
        for i in range(5):
            q.enqueue(Message(topic="t", value=f"task-{i}".encode()))
        for i in range(5):
            msg = q.dequeue()
            assert msg.value_str == f"task-{i}"

    def test_dequeue_empty(self):
        q = MessageQueue("tasks")
        assert q.dequeue() is None

    def test_depth(self):
        q = MessageQueue("tasks")
        for i in range(3):
            q.enqueue(Message(topic="t", value=b"v"))
        assert q.depth == 3

    def test_max_size_bounded(self):
        q = MessageQueue("tasks", max_size=2)
        q.enqueue(Message(topic="t", value=b"1"))
        q.enqueue(Message(topic="t", value=b"2"))
        with pytest.raises(ValueError, match="full"):
            q.enqueue(Message(topic="t", value=b"3"))

    def test_acknowledge(self):
        q = MessageQueue("tasks", visibility_timeout=30.0)
        q.enqueue(Message(topic="t", value=b"v"))
        msg = q.dequeue()
        result = q.acknowledge(msg.id)
        assert result is True

    def test_purge(self):
        q = MessageQueue("tasks")
        for i in range(5):
            q.enqueue(Message(topic="t", value=b"v"))
        count = q.purge()
        assert count == 5
        assert q.depth == 0

    def test_peek(self):
        q = MessageQueue("tasks")
        msg = Message(topic="t", value=b"hello")
        q.enqueue(msg)
        peeked = q.peek()
        assert peeked is not None
        assert peeked.value == b"hello"
        assert q.depth == 1  # not removed

    def test_total_enqueued_via_snapshot(self):
        q = MessageQueue("tasks")
        for i in range(3):
            q.enqueue(Message(topic="t", value=b"v"))
        snap = q.snapshot()
        assert snap["total_enqueued"] == 3

    def test_snapshot(self):
        q = MessageQueue("tasks")
        q.enqueue(Message(topic="t", value=b"v"))
        snap = q.snapshot()
        assert snap["name"] == "tasks"
        assert snap["depth"] == 1


class TestPriorityQueue:
    def test_priority_order(self):
        q = PriorityQueue("pq")
        q.enqueue(Message(topic="t", value=b"low", priority=1))
        q.enqueue(Message(topic="t", value=b"high", priority=10))
        q.enqueue(Message(topic="t", value=b"med", priority=5))
        msg = q.dequeue()
        assert msg.value == b"high"

    def test_same_priority_fifo(self):
        q = PriorityQueue("pq")
        q.enqueue(Message(topic="t", value=b"first", priority=5))
        q.enqueue(Message(topic="t", value=b"second", priority=5))
        msg = q.dequeue()
        assert msg.value == b"first"

    def test_empty_dequeue(self):
        q = PriorityQueue("pq")
        assert q.dequeue() is None

    def test_depth(self):
        q = PriorityQueue("pq")
        for i in range(3):
            q.enqueue(Message(topic="t", value=b"v", priority=i))
        assert q.depth == 3

    def test_max_size(self):
        q = PriorityQueue("pq", max_size=2)
        q.enqueue(Message(topic="t", value=b"1"))
        q.enqueue(Message(topic="t", value=b"2"))
        with pytest.raises(ValueError, match="full"):
            q.enqueue(Message(topic="t", value=b"3"))
