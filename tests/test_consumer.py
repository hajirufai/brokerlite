"""Tests for Consumer and ConsumerGroup."""

import pytest
from brokerlite.consumer import (
    Consumer, ConsumerConfig, ConsumerGroup, AssignmentStrategy,
)
from brokerlite.partition import Partition
from brokerlite.message import Message


class TestConsumer:
    def test_create_default(self):
        c = Consumer()
        assert c.consumer_id is not None
        assert len(c.assigned_partitions) == 0

    def test_create_custom_id(self):
        c = Consumer(consumer_id="consumer-1")
        assert c.consumer_id == "consumer-1"

    def test_assign_partition(self):
        c = Consumer()
        p = Partition("orders", 0)
        c.assign([p])
        assert len(c.assigned_partitions) == 1

    def test_revoke_partitions(self):
        c = Consumer()
        p = Partition("orders", 0)
        c.assign([p])
        c.revoke()
        assert len(c.assigned_partitions) == 0

    def test_poll_with_messages(self):
        c = Consumer()
        p = Partition("orders", 0)
        for i in range(5):
            p.append(Message(topic="orders", value=f"m{i}".encode()))
        c.assign([p])
        messages = c.poll(10)
        assert len(messages) == 5

    def test_poll_empty(self):
        c = Consumer()
        p = Partition("orders", 0)
        c.assign([p])
        messages = c.poll(10)
        assert len(messages) == 0

    def test_commit(self):
        c = Consumer(config=ConsumerConfig(group_id="g1"))
        p = Partition("orders", 0)
        p.append(Message(topic="orders", value=b"v"))
        c.assign([p])
        c.poll(10)
        committed = c.commit()
        assert len(committed) > 0

    def test_position(self):
        c = Consumer()
        p = Partition("orders", 0)
        p.append(Message(topic="orders", value=b"v"))
        c.assign([p])
        c.poll(10)
        pos = c.position("orders", 0)
        assert pos == 1

    def test_seek(self):
        c = Consumer()
        p = Partition("orders", 0)
        for i in range(10):
            p.append(Message(topic="orders", value=f"m{i}".encode()))
        c.assign([p])
        c.seek("orders", 0, 5)
        messages = c.poll(10)
        assert len(messages) == 5
        assert messages[0].value_str == "m5"

    def test_is_alive(self):
        c = Consumer()
        assert c.is_alive
        c.close()
        assert not c.is_alive


class TestConsumerGroup:
    def test_create(self):
        g = ConsumerGroup("group-1")
        assert g.group_id == "group-1"
        assert g.member_count == 0

    def test_join(self):
        g = ConsumerGroup("g1")
        c = Consumer()
        g.join(c)
        assert g.member_count == 1

    def test_leave(self):
        g = ConsumerGroup("g1")
        c = Consumer(consumer_id="c1")
        g.join(c)
        g.leave("c1")
        assert g.member_count == 0

    def test_subscribe_and_rebalance(self):
        g = ConsumerGroup("g1")
        p0 = Partition("orders", 0)
        p1 = Partition("orders", 1)
        c1 = Consumer(consumer_id="c1")
        c2 = Consumer(consumer_id="c2")

        g.join(c1)
        g.join(c2)
        g.subscribe("orders", [p0, p1])

        total_assigned = sum(
            len(c.assigned_partitions) for c in [c1, c2]
        )
        assert total_assigned == 2

    def test_range_assignment(self):
        g = ConsumerGroup("g1", strategy=AssignmentStrategy.RANGE)
        partitions = [Partition("t", i) for i in range(4)]
        consumers = [Consumer(consumer_id=f"c{i}") for i in range(2)]
        for c in consumers:
            g.join(c)
        g.subscribe("t", partitions)

        # Range: c0 gets [0,1], c1 gets [2,3]
        assert len(consumers[0].assigned_partitions) == 2
        assert len(consumers[1].assigned_partitions) == 2

    def test_round_robin_assignment(self):
        g = ConsumerGroup("g1", strategy=AssignmentStrategy.ROUND_ROBIN)
        partitions = [Partition("t", i) for i in range(4)]
        consumers = [Consumer(consumer_id=f"c{i}") for i in range(2)]
        for c in consumers:
            g.join(c)
        g.subscribe("t", partitions)

        assert len(consumers[0].assigned_partitions) == 2
        assert len(consumers[1].assigned_partitions) == 2

    def test_snapshot(self):
        g = ConsumerGroup("g1")
        c = Consumer(consumer_id="c1")
        g.join(c)
        snap = g.snapshot()
        assert snap["group_id"] == "g1"
        assert snap["member_count"] == 1
