"""Tests for the Message model."""

import time
import pytest
from brokerlite.message import Message, MessageBatch, MessageHeaders


class TestMessageHeaders:
    def test_set_and_get(self):
        h = MessageHeaders()
        h.set("key1", "val1")
        assert h.get("key1") == "val1"

    def test_get_default(self):
        h = MessageHeaders()
        assert h.get("missing", "default") == "default"
        assert h.get("missing") is None

    def test_remove(self):
        h = MessageHeaders({"a": "1"})
        assert h.remove("a") == "1"
        assert h.get("a") is None

    def test_has(self):
        h = MessageHeaders({"x": "y"})
        assert h.has("x")
        assert not h.has("z")

    def test_keys_and_items(self):
        h = MessageHeaders({"a": "1", "b": "2"})
        assert set(h.keys()) == {"a", "b"}
        assert set(h.items()) == {("a", "1"), ("b", "2")}

    def test_to_dict_and_from_dict(self):
        h = MessageHeaders({"k": "v"})
        d = h.to_dict()
        h2 = MessageHeaders.from_dict(d)
        assert h == h2

    def test_equality(self):
        h1 = MessageHeaders({"a": "1"})
        h2 = MessageHeaders({"a": "1"})
        assert h1 == h2

    def test_len(self):
        h = MessageHeaders({"a": "1", "b": "2"})
        assert len(h) == 2


class TestMessage:
    def test_create_basic(self):
        msg = Message(topic="orders", value=b"hello")
        assert msg.topic == "orders"
        assert msg.value == b"hello"
        assert msg.partition == -1
        assert msg.offset == -1

    def test_string_value_converted_to_bytes(self):
        msg = Message(topic="t", value="hello")
        assert msg.value == b"hello"

    def test_dict_headers_converted(self):
        msg = Message(topic="t", value=b"v", headers={"k": "v"})
        assert isinstance(msg.headers, MessageHeaders)
        assert msg.headers.get("k") == "v"

    def test_value_str(self):
        msg = Message(topic="t", value=b"world")
        assert msg.value_str == "world"

    def test_size_bytes(self):
        msg = Message(topic="test", value=b"data", key="k1")
        assert msg.size_bytes > 0

    def test_is_expired_no_ttl(self):
        msg = Message(topic="t", value=b"v", ttl=0)
        assert not msg.is_expired()

    def test_is_expired_with_ttl(self):
        msg = Message(topic="t", value=b"v", ttl=1)
        msg.timestamp = time.time() - 2
        assert msg.is_expired()

    def test_is_not_expired(self):
        msg = Message(topic="t", value=b"v", ttl=3600)
        assert not msg.is_expired()

    def test_to_dict_and_from_dict(self):
        msg = Message(
            topic="orders",
            value=b"purchase",
            key="user-1",
            headers={"type": "purchase"},
            priority=5,
            ttl=60,
        )
        d = msg.to_dict()
        msg2 = Message.from_dict(d)
        assert msg2.topic == "orders"
        assert msg2.value == b"purchase"
        assert msg2.key == "user-1"
        assert msg2.priority == 5
        assert msg2.ttl == 60
        assert msg2.headers.get("type") == "purchase"

    def test_to_bytes_and_from_bytes(self):
        msg = Message(topic="t", value=b"bytes_test", key="k")
        raw = msg.to_bytes()
        msg2 = Message.from_bytes(raw)
        assert msg2.topic == "t"
        assert msg2.value == b"bytes_test"
        assert msg2.key == "k"

    def test_clone(self):
        msg = Message(topic="t", value=b"original", key="k")
        cloned = msg.clone(topic="new_topic")
        assert cloned.topic == "new_topic"
        assert cloned.value == b"original"
        assert cloned.id != msg.id or cloned.topic != msg.topic

    def test_equality_by_id(self):
        msg1 = Message(topic="t", value=b"v", id="abc")
        msg2 = Message(topic="t", value=b"v", id="abc")
        assert msg1 == msg2

    def test_inequality(self):
        msg1 = Message(topic="t", value=b"v")
        msg2 = Message(topic="t", value=b"v")
        assert msg1 != msg2  # different auto-generated IDs

    def test_hash(self):
        msg = Message(topic="t", value=b"v", id="xyz")
        assert hash(msg) == hash("xyz")


class TestMessageBatch:
    def test_empty_batch(self):
        batch = MessageBatch()
        assert batch.size == 0
        assert len(batch) == 0

    def test_add_messages(self):
        batch = MessageBatch()
        batch.add(Message(topic="t", value=b"v1"))
        batch.add(Message(topic="t", value=b"v2"))
        assert batch.size == 2

    def test_extend(self):
        batch = MessageBatch()
        msgs = [Message(topic="t", value=b"v") for _ in range(5)]
        batch.extend(msgs)
        assert batch.size == 5

    def test_total_bytes(self):
        batch = MessageBatch([
            Message(topic="t", value=b"hello"),
            Message(topic="t", value=b"world"),
        ])
        assert batch.total_bytes > 0

    def test_by_topic(self):
        batch = MessageBatch([
            Message(topic="a", value=b"1"),
            Message(topic="b", value=b"2"),
            Message(topic="a", value=b"3"),
        ])
        groups = batch.by_topic()
        assert len(groups["a"]) == 2
        assert len(groups["b"]) == 1

    def test_by_partition(self):
        msg1 = Message(topic="t", value=b"1", partition=0)
        msg1.partition = 0
        msg2 = Message(topic="t", value=b"2", partition=1)
        msg2.partition = 1
        batch = MessageBatch([msg1, msg2])
        groups = batch.by_partition()
        assert ("t", 0) in groups
        assert ("t", 1) in groups

    def test_clear(self):
        batch = MessageBatch([Message(topic="t", value=b"v")])
        batch.clear()
        assert batch.size == 0

    def test_to_bytes_and_from_bytes(self):
        batch = MessageBatch([
            Message(topic="t1", value=b"v1"),
            Message(topic="t2", value=b"v2"),
        ])
        raw = batch.to_bytes()
        batch2 = MessageBatch.from_bytes(raw)
        assert batch2.size == 2

    def test_iter(self):
        msgs = [Message(topic="t", value=b"v") for _ in range(3)]
        batch = MessageBatch(msgs)
        count = 0
        for _ in batch:
            count += 1
        assert count == 3
