"""Tests for Topic management."""

import pytest
from brokerlite.topic import Topic, TopicConfig
from brokerlite.message import Message


class TestTopicConfig:
    def test_defaults(self):
        config = TopicConfig()
        assert config.num_partitions == 4
        assert config.retention_ms == 0
        assert config.max_message_bytes == 0
        assert not config.compaction_enabled


class TestTopic:
    def test_create_default(self):
        topic = Topic("orders")
        assert topic.name == "orders"
        assert topic.num_partitions == 4

    def test_create_custom_partitions(self):
        topic = Topic("events", TopicConfig(num_partitions=8))
        assert topic.num_partitions == 8

    def test_get_partition(self):
        topic = Topic("t", TopicConfig(num_partitions=4))
        p = topic.get_partition(2)
        assert p.partition_id == 2

    def test_get_invalid_partition(self):
        topic = Topic("t", TopicConfig(num_partitions=2))
        with pytest.raises(ValueError):
            topic.get_partition(5)

    def test_assign_partition_round_robin(self):
        topic = Topic("t", TopicConfig(num_partitions=4))
        partitions = [topic.assign_partition(None) for _ in range(8)]
        assert partitions == [0, 1, 2, 3, 0, 1, 2, 3]

    def test_assign_partition_by_key(self):
        topic = Topic("t", TopicConfig(num_partitions=4))
        p1 = topic.assign_partition("user-123")
        p2 = topic.assign_partition("user-123")
        assert p1 == p2  # same key always maps to same partition

    def test_different_keys_may_differ(self):
        topic = Topic("t", TopicConfig(num_partitions=100))
        partitions = {topic.assign_partition(f"key-{i}") for i in range(20)}
        assert len(partitions) > 1  # different keys should spread

    def test_publish_message(self):
        topic = Topic("orders", TopicConfig(num_partitions=2))
        msg = Message(topic="orders", value=b"data", key="k")
        offset = topic.publish(msg)
        assert offset == 0
        assert msg.partition >= 0

    def test_publish_assigns_partition(self):
        topic = Topic("t", TopicConfig(num_partitions=4))
        msg = Message(topic="t", value=b"v")
        topic.publish(msg)
        assert msg.partition >= 0
        assert msg.partition < 4

    def test_publish_max_message_bytes(self):
        topic = Topic("t", TopicConfig(max_message_bytes=10))
        msg = Message(topic="t", value=b"x" * 100)
        with pytest.raises(ValueError, match="exceeds max"):
            topic.publish(msg)

    def test_subscribe_unsubscribe(self):
        topic = Topic("t")
        topic.subscribe("group-1")
        assert topic.subscriber_count == 1
        assert "group-1" in topic.subscribers
        topic.unsubscribe("group-1")
        assert topic.subscriber_count == 0

    def test_total_messages(self):
        topic = Topic("t", TopicConfig(num_partitions=2))
        for i in range(5):
            topic.publish(Message(topic="t", value=f"msg-{i}".encode()))
        assert topic.total_messages() == 5

    def test_snapshot(self):
        topic = Topic("orders", TopicConfig(num_partitions=2))
        topic.publish(Message(topic="orders", value=b"data"))
        snap = topic.snapshot()
        assert snap["name"] == "orders"
        assert snap["num_partitions"] == 2
        assert snap["total_messages"] == 1

    def test_compact_removes_old_keys(self):
        topic = Topic("t", TopicConfig(num_partitions=1, compaction_enabled=True))
        for i in range(5):
            topic.publish(Message(topic="t", value=f"v{i}".encode(), key="same-key"))
        removed = topic.compact()
        assert removed == 4
        assert topic.total_messages() == 1
