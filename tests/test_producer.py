"""Tests for the Producer."""

import time
import pytest
from brokerlite.producer import Producer, ProducerConfig, RecordMetadata
from brokerlite.topic import Topic, TopicConfig
from brokerlite.message import Message


class TestProducer:
    def _make_topic(self, name="orders", partitions=4):
        return Topic(name, TopicConfig(num_partitions=partitions))

    def test_create_default(self):
        p = Producer()
        assert p.producer_id is not None

    def test_send_without_publish_fn(self):
        """Without a publish_fn, producer returns metadata with offset=-1."""
        p = Producer(config=ProducerConfig(batch_size=1))
        metadata = p.send("orders", b"order-data", key="k1")
        assert metadata is not None
        assert metadata.topic == "orders"
        assert metadata.offset == -1

    def test_send_with_publish_fn(self):
        topic = self._make_topic()

        def publish_fn(msg):
            offset = topic.publish(msg)
            return RecordMetadata(
                topic=msg.topic, partition=msg.partition,
                offset=offset, timestamp=time.time(),
                message_id=msg.id,
            )

        p = Producer(
            config=ProducerConfig(batch_size=1),
            publish_fn=publish_fn,
        )
        metadata = p.send("orders", b"order-data", key="k1")
        assert metadata is not None
        assert metadata.topic == "orders"
        assert metadata.offset == 0

    def test_send_with_batching(self):
        topic = self._make_topic()

        def publish_fn(msg):
            offset = topic.publish(msg)
            return RecordMetadata(
                topic=msg.topic, partition=msg.partition,
                offset=offset, timestamp=time.time(),
                message_id=msg.id,
            )

        p = Producer(
            config=ProducerConfig(batch_size=3, linger_ms=10000),
            publish_fn=publish_fn,
        )
        r1 = p.send("orders", b"v1")
        r2 = p.send("orders", b"v2")
        assert r1 is None  # batched
        assert r2 is None  # batched
        assert p.pending_count == 2

        results = p.flush()
        assert len(results) >= 1
        assert p.pending_count == 0

    def test_idempotent_sequence_numbers(self):
        p = Producer(config=ProducerConfig(enable_idempotence=True, batch_size=1))
        p.send("orders", b"v1")
        p.send("orders", b"v2")
        assert p.snapshot()["sequence_number"] == 2

    def test_callbacks_success(self):
        topic = self._make_topic()
        success_called = []

        def publish_fn(msg):
            offset = topic.publish(msg)
            return RecordMetadata(
                topic=msg.topic, partition=msg.partition,
                offset=offset, timestamp=time.time(),
                message_id=msg.id,
            )

        p = Producer(
            config=ProducerConfig(batch_size=1),
            publish_fn=publish_fn,
        )
        p.on_success(lambda m: success_called.append(m))
        p.send("orders", b"data")
        assert len(success_called) == 1

    def test_callbacks_error(self):
        errors = []

        def failing_publish(msg):
            raise RuntimeError("Publish failed")

        p = Producer(
            config=ProducerConfig(batch_size=1),
            publish_fn=failing_publish,
        )
        p.on_error(lambda msg, e: errors.append((msg, e)))
        result = p.send("orders", b"data")
        assert result is None
        assert len(errors) == 1

    def test_record_metadata(self):
        rm = RecordMetadata(
            topic="orders", partition=2, offset=42,
            timestamp=time.time(), message_id="abc",
        )
        assert rm.topic == "orders"
        assert rm.partition == 2
        assert rm.offset == 42

    def test_close(self):
        p = Producer()
        p.close()
        assert p._closed

    def test_send_after_close_raises(self):
        p = Producer()
        p.close()
        with pytest.raises(RuntimeError, match="closed"):
            p.send("orders", b"data")

    def test_snapshot(self):
        p = Producer()
        snap = p.snapshot()
        assert "producer_id" in snap
        assert "total_sent" in snap
