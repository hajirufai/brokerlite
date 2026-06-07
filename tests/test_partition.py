"""Tests for Partition."""

import time
import pytest
from brokerlite.partition import Partition
from brokerlite.message import Message


class TestPartition:
    def test_create(self):
        p = Partition("orders", 0)
        assert p.topic == "orders"
        assert p.partition_id == 0
        assert p.size == 0

    def test_append_and_read(self):
        p = Partition("t", 0)
        msg = Message(topic="t", value=b"hello")
        offset = p.append(msg)
        assert offset == 0
        messages = p.read(0)
        assert len(messages) == 1
        assert messages[0].value == b"hello"

    def test_sequential_offsets(self):
        p = Partition("t", 0)
        for i in range(5):
            offset = p.append(Message(topic="t", value=f"msg-{i}".encode()))
            assert offset == i

    def test_read_from_offset(self):
        p = Partition("t", 0)
        for i in range(10):
            p.append(Message(topic="t", value=f"msg-{i}".encode()))
        messages = p.read(5)
        assert len(messages) == 5
        assert messages[0].value_str == "msg-5"

    def test_read_max_messages(self):
        p = Partition("t", 0)
        for i in range(10):
            p.append(Message(topic="t", value=f"msg-{i}".encode()))
        messages = p.read(0, max_messages=3)
        assert len(messages) == 3

    def test_read_empty(self):
        p = Partition("t", 0)
        messages = p.read(0)
        assert len(messages) == 0

    def test_read_beyond_end(self):
        p = Partition("t", 0)
        p.append(Message(topic="t", value=b"v"))
        messages = p.read(100)
        assert len(messages) == 0

    def test_committed_offset_default(self):
        p = Partition("t", 0)
        assert p.get_committed_offset("group-1") == 0

    def test_commit_and_fetch_offset(self):
        p = Partition("t", 0)
        p.commit_offset("group-1", 5)
        assert p.get_committed_offset("group-1") == 5

    def test_current_offset(self):
        p = Partition("t", 0)
        assert p.current_offset == 0
        p.append(Message(topic="t", value=b"v"))
        assert p.current_offset == 1
        p.append(Message(topic="t", value=b"v"))
        assert p.current_offset == 2

    def test_size_bytes(self):
        p = Partition("t", 0)
        p.append(Message(topic="t", value=b"hello"))
        assert p.size_bytes > 0

    def test_retention(self):
        p = Partition("t", 0)
        for i in range(3):
            msg = Message(topic="t", value=f"msg-{i}".encode())
            p.append(msg)
            # manually set broker_timestamp to old
            p._log[-1].broker_timestamp = time.time() - 3600
        for i in range(2):
            p.append(Message(topic="t", value=f"recent-{i}".encode()))
        # retention_ms is set at Partition level, but apply_retention checks self.retention_ms
        p.retention_ms = 1800000  # 30 min
        removed = p.apply_retention()
        assert removed == 3
        assert p.size == 2

    def test_compaction(self):
        p = Partition("t", 0)
        for i in range(5):
            p.append(Message(topic="t", value=f"v{i}".encode(), key="same"))
        removed = p.compact()
        assert removed == 4
        assert p.size == 1
        messages = p.read(0)
        assert messages[0].value_str == "v4"

    def test_snapshot(self):
        p = Partition("t", 0)
        p.append(Message(topic="t", value=b"v"))
        snap = p.snapshot()
        assert snap["topic"] == "t"
        assert snap["partition_id"] == 0
        assert snap["message_count"] == 1
