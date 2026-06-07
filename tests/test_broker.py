"""Tests for the central Broker engine."""

import time
import pytest
from brokerlite.broker import Broker, BrokerConfig
from brokerlite.consumer import Consumer, ConsumerGroup, AssignmentStrategy
from brokerlite.message import Message
from brokerlite.topic import TopicConfig


class TestBroker:
    def setup_method(self):
        self.broker = Broker()
        self.broker.start()

    def teardown_method(self):
        self.broker.stop()

    def test_create_topic(self):
        self.broker.create_topic("orders")
        assert "orders" in self.broker.list_topics()

    def test_create_topic_custom_config(self):
        config = TopicConfig(num_partitions=8)
        self.broker.create_topic("events", config)
        topic = self.broker.get_topic("events")
        assert topic.num_partitions == 8

    def test_duplicate_topic_raises(self):
        self.broker.create_topic("orders")
        with pytest.raises(ValueError, match="already exists"):
            self.broker.create_topic("orders")

    def test_delete_topic(self):
        self.broker.create_topic("orders")
        assert self.broker.delete_topic("orders")
        assert "orders" not in self.broker.list_topics()

    def test_delete_nonexistent_topic(self):
        assert not self.broker.delete_topic("nope")

    def test_publish(self):
        self.broker.create_topic("orders")
        msg = Message(topic="orders", value=b"order-data")
        metadata = self.broker.publish(msg)
        assert metadata.topic == "orders"
        assert metadata.offset >= 0

    def test_publish_auto_creates_topic(self):
        """Publishing to an unknown topic auto-creates it."""
        msg = Message(topic="auto-created", value=b"data")
        metadata = self.broker.publish(msg)
        assert metadata.topic == "auto-created"
        assert "auto-created" in self.broker.list_topics()

    def test_create_queue(self):
        self.broker.create_queue("tasks")
        assert "tasks" in self.broker.list_queues()

    def test_enqueue_dequeue(self):
        self.broker.create_queue("tasks")
        msg = Message(topic="tasks", value=b"task-data")
        self.broker.enqueue("tasks", msg)
        q = self.broker.get_queue("tasks")
        result = q.dequeue()
        assert result.value == b"task-data"

    def test_subscribe(self):
        self.broker.create_topic("orders")
        consumer = Consumer(consumer_id="c1")
        self.broker.subscribe("group-1", "orders", consumer)
        groups = self.broker.list_consumer_groups()
        assert "group-1" in groups

    def test_consumer_group_created_on_subscribe(self):
        self.broker.create_topic("orders")
        c = Consumer()
        self.broker.subscribe("g1", "orders", c)
        group = self.broker.get_consumer_group("g1")
        assert group is not None
        assert group.member_count == 1

    def test_create_consumer_group(self):
        group = self.broker.create_consumer_group("g1")
        assert group.group_id == "g1"

    def test_delete_consumer_group(self):
        self.broker.create_consumer_group("g1")
        assert self.broker.delete_consumer_group("g1")
        assert "g1" not in self.broker.list_consumer_groups()

    def test_snapshot(self):
        self.broker.create_topic("orders")
        self.broker.publish(Message(topic="orders", value=b"v"))
        snap = self.broker.snapshot()
        assert "topics" in snap
        assert len(snap["topics"]) == 1
        assert "orders" in snap["topics"]

    def test_apply_retention(self):
        self.broker.create_topic("orders", TopicConfig(retention_ms=1))
        topic = self.broker.get_topic("orders")
        msg = Message(topic="orders", value=b"old")
        p = topic.get_partition(0)
        p.append(msg)
        # Set broker_timestamp in the past so retention removes it
        p._log[-1].broker_timestamp = time.time() - 3600
        result = self.broker.apply_retention()
        assert isinstance(result, dict)
