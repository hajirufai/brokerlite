"""Dashboard demo — generate an HTML dashboard for broker metrics."""

import time
from brokerlite.broker import Broker
from brokerlite.consumer import Consumer, ConsumerConfig
from brokerlite.message import Message
from brokerlite.topic import TopicConfig
from brokerlite.metrics import MetricsCollector


def main():
    broker = Broker()
    broker.start()
    metrics = MetricsCollector(window_seconds=60)

    # Set up topics and queues
    broker.create_topic("orders", TopicConfig(num_partitions=4))
    broker.create_topic("events", TopicConfig(num_partitions=2))
    broker.create_queue("tasks")
    print("✓ Created 2 topics + 1 queue")

    # Generate traffic
    for i in range(50):
        msg = Message(
            topic="orders",
            value=f'{{"order_id": {i}, "total": {(i + 1) * 9.99:.2f}}}'.encode(),
            key=f"customer-{i % 10}",
        )
        start = time.monotonic()
        broker.publish(msg)
        latency_ms = (time.monotonic() - start) * 1000
        metrics.record_message_in()
        metrics.record_latency(latency_ms)

    for i in range(20):
        msg = Message(
            topic="events",
            value=f'{{"type": "click", "page": "/product/{i}"}}'.encode(),
        )
        broker.publish(msg)
        metrics.record_message_in()

    # Subscribe consumers
    c1 = Consumer(consumer_id="order-svc", config=ConsumerConfig(group_id="order-processors"))
    broker.subscribe("order-processors", "orders", c1)
    consumed = c1.poll(30)
    for _ in consumed:
        metrics.record_message_out()
    c1.commit()

    # Update metrics
    for name in broker.list_topics():
        topic = broker.get_topic(name)
        if topic:
            metrics.update_topic_depth(name, topic.total_messages())
    metrics.update_consumer_lag("order-processors", 20)
    metrics.update_connections(2)

    # Take snapshot
    snap = metrics.snapshot()
    snap_dict = snap.to_dict()
    print(f"\n--- Metrics Snapshot ---")
    print(f"  Messages in rate: {snap_dict['messages_in_rate']}/s")
    print(f"  Messages out rate: {snap_dict['messages_out_rate']}/s")
    print(f"  Total messages: {snap_dict['total_messages']}")
    print(f"  Topic depths: {snap_dict['topic_depths']}")
    print(f"  Consumer lag: {snap_dict['consumer_lag']}")
    print(f"  Latency p50: {snap_dict['latency_ms']['p50']:.3f}ms")
    print(f"  Latency p99: {snap_dict['latency_ms']['p99']:.3f}ms")

    # Broker snapshot
    broker_snap = broker.snapshot()
    print(f"\n--- Broker State ---")
    print(f"  Topics: {list(broker_snap['topics'].keys())}")
    print(f"  Queues: {list(broker_snap['queues'].keys())}")
    print(f"  Consumer groups: {list(broker_snap['consumer_groups'].keys())}")
    print(f"  Total messages in: {broker_snap['total_messages_in']}")

    broker.stop()
    print("\n✓ Done — broker stopped")


if __name__ == "__main__":
    main()
