"""Dead letter queue example — handling permanently failed messages."""

from brokerlite.message import Message
from brokerlite.ack import AckManager, AckMode
from brokerlite.dlq import DeadLetterQueue
from brokerlite.retry import RetryPolicy, RetryStrategy


def main():
    # Configure retry and DLQ
    dlq = DeadLetterQueue("orders-dlq", max_size=1000)
    retry_policy = RetryPolicy(
        max_attempts=3,
        initial_delay=0.5,
        backoff_multiplier=2.0,
        strategy=RetryStrategy.EXPONENTIAL,
    )

    def on_dead_letter(msg, reason):
        dlq.add(msg, reason, attempts=3)

    ack_mgr = AckManager(
        ack_timeout=5.0,
        max_attempts=3,
        on_dead_letter=on_dead_letter,
    )

    print("Retry policy:")
    print(f"  Strategy: {retry_policy.strategy.value}")
    print(f"  Max attempts: {retry_policy.max_attempts}")
    print(f"  Delays: {[f'{d:.1f}s' for d in retry_policy.delays_for_all_attempts()]}")
    print(f"  Max total delay: {retry_policy.total_max_delay():.1f}s\n")

    # Simulate processing messages with failures
    messages = [
        Message(topic="orders", value=b'{"order": 1, "status": "ok"}', id="msg-1"),
        Message(topic="orders", value=b'{"order": 2, "status": "invalid_card"}', id="msg-2"),
        Message(topic="orders", value=b'{"order": 3, "status": "ok"}', id="msg-3"),
        Message(topic="orders", value=b'{"order": 4, "status": "timeout"}', id="msg-4"),
    ]

    for msg in messages:
        data = msg.value_str
        is_failure = "invalid_card" in data or "timeout" in data

        if is_failure:
            # Simulate retry attempts
            for attempt in range(1, retry_policy.max_attempts + 1):
                result = retry_policy.should_retry(attempt)
                if result.should_retry:
                    print(f"  ⟳ Retry {msg.id}: attempt {attempt}, "
                          f"delay={result.delay_seconds:.1f}s")
                else:
                    print(f"  ✗ {msg.id}: {result.reason}")
                    # Route to DLQ
                    ack_mgr.track(msg, "worker-1")
                    for _ in range(ack_mgr.max_attempts):
                        ack_mgr.track(msg, "worker-1")
                    ack_mgr.negative_acknowledge(msg.id)
                    break
        else:
            print(f"  ✓ {msg.id}: processed successfully")

    # Inspect the DLQ
    print(f"\n--- Dead Letter Queue ---")
    print(f"Size: {dlq.size}")
    print(f"Failure reasons: {dlq.failure_reasons()}")

    entries = dlq.peek(10)
    for entry in entries:
        print(f"\n  Message: {entry.message.id}")
        print(f"  Topic: {entry.original_topic}")
        print(f"  Reason: {entry.reason}")
        print(f"  Attempts: {entry.attempts}")
        print(f"  Value: {entry.message.value_str}")

    # Replay messages from DLQ
    print(f"\n--- Replaying DLQ ---")
    replayed = dlq.replay()
    print(f"Replayed {len(replayed)} message(s)")
    for msg in replayed:
        print(f"  → {msg.id}: {msg.value_str}")

    print(f"\nDLQ size after replay: {dlq.size}")
    print(f"Ack manager snapshot: {ack_mgr.snapshot()}")


if __name__ == "__main__":
    main()
