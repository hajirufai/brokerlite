"""Work queue example — point-to-point message delivery with acknowledgments."""

import time
from brokerlite.broker import Broker
from brokerlite.message import Message
from brokerlite.ack import AckManager
from brokerlite.dlq import DeadLetterQueue


def main():
    broker = Broker()
    broker.start()

    # Create a work queue and dead letter queue
    broker.create_queue("tasks", max_size=1000)
    dlq = DeadLetterQueue("tasks-dlq")
    ack_mgr = AckManager(ack_timeout=5.0, max_attempts=3)
    print("✓ Created work queue 'tasks' + DLQ")

    # Enqueue work items
    tasks = [
        ("resize-img-001", 5),
        ("send-email-042", 3),
        ("generate-report", 8),
        ("sync-database", 2),
        ("process-payment", 9),
    ]

    for name, priority in tasks:
        msg = Message(topic="tasks", value=name.encode(), priority=priority)
        broker.enqueue("tasks", msg)
        print(f"  Enqueued: {name} (priority={priority})")

    # Process work items
    q = broker.get_queue("tasks")
    print(f"\n✓ Processing {q.depth} tasks:")

    processed = 0
    while True:
        msg = q.dequeue()
        if msg is None:
            break

        task_name = msg.value_str
        ack_mgr.track(msg, "worker-1")

        # Simulate processing (fail "process-payment" to demo DLQ)
        if "payment" in task_name:
            print(f"  ✗ Failed: {task_name}")
            result = ack_mgr.negative_acknowledge(msg.id)
            if result is None:
                dlq.add(msg, "Simulated payment failure", attempts=3)
                print(f"    → Moved to DLQ")
            continue

        ack_mgr.acknowledge(msg.id)
        processed += 1
        print(f"  ✓ Processed: {task_name}")

    # Stats
    print(f"\nResults:")
    print(f"  Processed: {processed}")
    print(f"  DLQ size: {dlq.size}")
    print(f"  Ack manager: {ack_mgr.snapshot()}")

    # Replay DLQ
    replayed = dlq.replay()
    if replayed:
        print(f"\n✓ Replayed {len(replayed)} message(s) from DLQ")
        for msg in replayed:
            print(f"  → {msg.value_str}")

    broker.stop()


if __name__ == "__main__":
    main()
