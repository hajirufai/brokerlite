"""Basic pub/sub example — produce and consume messages through topics."""

from brokerlite.broker import Broker
from brokerlite.consumer import Consumer, ConsumerConfig
from brokerlite.message import Message
from brokerlite.topic import TopicConfig


def main():
    # Create and start the broker
    broker = Broker()
    broker.start()

    # Create a topic with 4 partitions
    broker.create_topic("orders", TopicConfig(num_partitions=4))
    print("✓ Created topic 'orders' with 4 partitions")

    # Publish messages
    for i in range(10):
        msg = Message(
            topic="orders",
            value=f'{{"order_id": {i}, "item": "widget-{i}"}}'.encode(),
            key=f"user-{i % 3}",
        )
        metadata = broker.publish(msg)
        print(f"  Published order {i} → partition {metadata.partition}, offset {metadata.offset}")

    # Subscribe a consumer
    consumer = Consumer(
        consumer_id="order-processor",
        config=ConsumerConfig(group_id="processors", max_poll_messages=20),
    )
    broker.subscribe("processors", "orders", consumer)

    # Poll and process messages
    messages = consumer.poll(max_messages=20)
    print(f"\n✓ Consumed {len(messages)} messages:")
    for msg in messages:
        print(f"  [{msg.topic}:{msg.partition}@{msg.offset}] key={msg.key} → {msg.value_str}")

    # Commit offsets
    committed = consumer.commit()
    print(f"\n✓ Committed offsets: {committed}")

    # Broker stats
    snap = broker.snapshot()
    print(f"\nBroker stats: {snap['total_messages_in']} messages in, "
          f"{len(snap['topics'])} topics, {len(snap['consumer_groups'])} groups")

    broker.stop()


if __name__ == "__main__":
    main()
