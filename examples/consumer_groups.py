"""Consumer groups — scalable parallel consumption with partition assignment."""

from brokerlite.broker import Broker
from brokerlite.consumer import Consumer, ConsumerConfig, AssignmentStrategy
from brokerlite.message import Message
from brokerlite.topic import TopicConfig


def main():
    broker = Broker()
    broker.start()

    # Create a topic with 6 partitions for parallel consumption
    broker.create_topic("events", TopicConfig(num_partitions=6))
    print("✓ Created topic 'events' with 6 partitions\n")

    # Publish 30 events
    event_types = ["click", "purchase", "signup", "view", "logout"]
    for i in range(30):
        etype = event_types[i % len(event_types)]
        msg = Message(
            topic="events",
            value=f'{{"type": "{etype}", "seq": {i}}}'.encode(),
            key=f"user-{i % 5}",
        )
        broker.publish(msg)
    print(f"✓ Published 30 events across 6 partitions\n")

    # Create 3 consumers in the same group — partitions are distributed
    consumers = []
    for i in range(3):
        c = Consumer(
            consumer_id=f"analytics-{i}",
            config=ConsumerConfig(group_id="analytics", max_poll_messages=50),
        )
        broker.subscribe("analytics", "events", c)
        consumers.append(c)
        print(f"  Consumer {c.consumer_id}: "
              f"assigned {len(c.assigned_partitions)} partitions "
              f"({[p.partition_id for p in c.assigned_partitions]})")

    # Each consumer polls its partitions
    print(f"\n✓ Consuming messages:")
    total = 0
    for c in consumers:
        messages = c.poll(max_messages=50)
        total += len(messages)
        print(f"  {c.consumer_id}: consumed {len(messages)} messages")
        c.commit()

    print(f"\nTotal consumed: {total}/30 messages")

    # Show consumer group state
    group = broker.get_consumer_group("analytics")
    snap = group.snapshot()
    print(f"\nGroup '{snap['group_id']}':")
    print(f"  Members: {snap['member_count']}")
    print(f"  Strategy: {snap['strategy']}")
    print(f"  Subscriptions: {snap['subscribed_topics']}")

    # Simulate a consumer leaving — triggers rebalance
    print(f"\n--- Consumer analytics-2 leaves ---")
    group.leave("analytics-2")
    print(f"  Group now has {group.member_count} members")

    # Remaining consumers get reassigned partitions
    for c in consumers[:2]:
        print(f"  {c.consumer_id}: "
              f"{len(c.assigned_partitions)} partitions "
              f"({[p.partition_id for p in c.assigned_partitions]})")

    broker.stop()


if __name__ == "__main__":
    main()
