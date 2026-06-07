"""CLI management tool for BrokerLite.

Provides commands for managing topics, queues, consumer groups,
and inspecting broker state.

Usage:
    python -m brokerlite.cli topics list
    python -m brokerlite.cli topics create --name orders --partitions 8
    python -m brokerlite.cli produce orders --key user123 --value '{"event": "buy"}'
    python -m brokerlite.cli consume my-group orders --max 10
    python -m brokerlite.cli metrics
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Optional

from .broker import Broker
from .admin import AdminManager
from .consumer import Consumer, ConsumerConfig
from .message import Message
from .topic import TopicConfig


def format_table(headers: list[str], rows: list[list[str]], min_width: int = 10) -> str:
    """Format data as an ASCII table."""
    widths = [max(min_width, len(h)) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            if i < len(widths):
                widths[i] = max(widths[i], len(str(cell)))

    divider = "+" + "+".join("-" * (w + 2) for w in widths) + "+"

    header_line = "|" + "|".join(
        f" {h:<{widths[i]}} " for i, h in enumerate(headers)
    ) + "|"

    lines = [divider, header_line, divider]
    for row in rows:
        cells = []
        for i, cell in enumerate(row):
            w = widths[i] if i < len(widths) else min_width
            cells.append(f" {str(cell):<{w}} ")
        lines.append("|" + "|".join(cells) + "|")
    lines.append(divider)

    return "\n".join(lines)


def format_json(data, indent: int = 2) -> str:
    """Pretty-print JSON data."""
    return json.dumps(data, indent=indent, default=str)


class CLI:
    """BrokerLite CLI handler."""

    def __init__(self, broker: Optional[Broker] = None):
        self.broker = broker or Broker()
        self.admin = AdminManager(self.broker)
        self.broker.start()

    def run(self, args: Optional[list[str]] = None) -> int:
        """Parse arguments and execute the command."""
        parser = self._build_parser()
        parsed = parser.parse_args(args)

        if not hasattr(parsed, "func"):
            parser.print_help()
            return 1

        try:
            return parsed.func(parsed)
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1

    def _build_parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(
            prog="brokerlite",
            description="BrokerLite — In-Memory Message Broker CLI",
        )
        subparsers = parser.add_subparsers(dest="command")

        # --- topics ---
        topics_parser = subparsers.add_parser("topics", help="Manage topics")
        topics_sub = topics_parser.add_subparsers(dest="action")

        list_parser = topics_sub.add_parser("list", help="List all topics")
        list_parser.set_defaults(func=self._topics_list)

        create_parser = topics_sub.add_parser("create", help="Create a topic")
        create_parser.add_argument("--name", required=True)
        create_parser.add_argument("--partitions", type=int, default=4)
        create_parser.add_argument("--retention-ms", type=int, default=0)
        create_parser.add_argument("--compaction", action="store_true")
        create_parser.set_defaults(func=self._topics_create)

        describe_parser = topics_sub.add_parser("describe", help="Describe a topic")
        describe_parser.add_argument("name")
        describe_parser.set_defaults(func=self._topics_describe)

        delete_parser = topics_sub.add_parser("delete", help="Delete a topic")
        delete_parser.add_argument("name")
        delete_parser.set_defaults(func=self._topics_delete)

        # --- queues ---
        queues_parser = subparsers.add_parser("queues", help="Manage queues")
        queues_sub = queues_parser.add_subparsers(dest="action")

        q_list = queues_sub.add_parser("list", help="List queues")
        q_list.set_defaults(func=self._queues_list)

        q_create = queues_sub.add_parser("create", help="Create a queue")
        q_create.add_argument("--name", required=True)
        q_create.add_argument("--max-size", type=int, default=0)
        q_create.add_argument("--priority", action="store_true")
        q_create.set_defaults(func=self._queues_create)

        # --- groups ---
        groups_parser = subparsers.add_parser("groups", help="Manage consumer groups")
        groups_sub = groups_parser.add_subparsers(dest="action")

        g_list = groups_sub.add_parser("list", help="List groups")
        g_list.set_defaults(func=self._groups_list)

        g_describe = groups_sub.add_parser("describe", help="Describe a group")
        g_describe.add_argument("group_id")
        g_describe.set_defaults(func=self._groups_describe)

        # --- produce ---
        produce_parser = subparsers.add_parser("produce", help="Produce a message")
        produce_parser.add_argument("topic")
        produce_parser.add_argument("--key")
        produce_parser.add_argument("--value", required=True)
        produce_parser.add_argument("--headers", type=json.loads, default={})
        produce_parser.set_defaults(func=self._produce)

        # --- consume ---
        consume_parser = subparsers.add_parser("consume", help="Consume messages")
        consume_parser.add_argument("group_id")
        consume_parser.add_argument("topic")
        consume_parser.add_argument("--max", type=int, default=10)
        consume_parser.add_argument("--from-beginning", action="store_true")
        consume_parser.set_defaults(func=self._consume)

        # --- metrics ---
        metrics_parser = subparsers.add_parser("metrics", help="Broker metrics")
        metrics_parser.set_defaults(func=self._metrics)

        # --- dlq ---
        dlq_parser = subparsers.add_parser("dlq", help="Dead letter queue operations")
        dlq_sub = dlq_parser.add_subparsers(dest="action")

        dlq_list = dlq_sub.add_parser("list", help="List DLQ entries")
        dlq_list.add_argument("topic")
        dlq_list.set_defaults(func=self._dlq_list)

        return parser

    # --- Command handlers ---

    def _topics_list(self, args) -> int:
        topics = self.admin.list_topics()
        if not topics:
            print("No topics found.")
            return 0
        rows = [
            [t["name"], str(t["partitions"]), str(t["messages"]),
             str(t["bytes"]), str(t["subscribers"])]
            for t in topics
        ]
        print(format_table(
            ["Name", "Partitions", "Messages", "Bytes", "Subscribers"],
            rows,
        ))
        return 0

    def _topics_create(self, args) -> int:
        result = self.admin.create_topic(
            name=args.name,
            partitions=args.partitions,
            retention_ms=args.retention_ms,
            compaction=args.compaction,
        )
        print(f"Topic {args.name!r} created with {args.partitions} partitions.")
        return 0

    def _topics_describe(self, args) -> int:
        info = self.admin.describe_topic(args.name)
        if not info:
            print(f"Topic {args.name!r} not found.")
            return 1
        print(format_json(info))
        return 0

    def _topics_delete(self, args) -> int:
        deleted = self.admin.delete_topic(args.name)
        if deleted:
            print(f"Topic {args.name!r} deleted.")
        else:
            print(f"Topic {args.name!r} not found.")
        return 0 if deleted else 1

    def _queues_list(self, args) -> int:
        queues = self.admin.list_queues()
        if not queues:
            print("No queues found.")
            return 0
        rows = [
            [q["name"], str(q["depth"]), str(q.get("in_flight", 0)),
             str(q["total_enqueued"])]
            for q in queues
        ]
        print(format_table(
            ["Name", "Depth", "In-Flight", "Total Enqueued"],
            rows,
        ))
        return 0

    def _queues_create(self, args) -> int:
        self.admin.create_queue(
            name=args.name,
            max_size=args.max_size,
            priority=args.priority,
        )
        print(f"Queue {args.name!r} created.")
        return 0

    def _groups_list(self, args) -> int:
        groups = self.admin.list_consumer_groups()
        if not groups:
            print("No consumer groups found.")
            return 0
        rows = [
            [g["group_id"], str(g["members"]), str(g["total_lag"])]
            for g in groups
        ]
        print(format_table(["Group ID", "Members", "Total Lag"], rows))
        return 0

    def _groups_describe(self, args) -> int:
        info = self.admin.describe_consumer_group(args.group_id)
        if not info:
            print(f"Consumer group {args.group_id!r} not found.")
            return 1
        print(format_json(info))
        return 0

    def _produce(self, args) -> int:
        msg = Message(
            topic=args.topic,
            value=args.value.encode("utf-8"),
            key=args.key,
            headers=args.headers,
        )
        metadata = self.broker.publish(msg)
        print(
            f"Produced to {metadata.topic} "
            f"partition={metadata.partition} "
            f"offset={metadata.offset}"
        )
        return 0

    def _consume(self, args) -> int:
        consumer = Consumer(config=ConsumerConfig(
            group_id=args.group_id,
            auto_commit=False,
            max_poll_messages=args.max,
        ))
        self.broker.subscribe(args.group_id, args.topic, consumer)

        if args.from_beginning:
            consumer.seek_to_beginning()

        messages = consumer.poll(args.max)
        if not messages:
            print("No messages available.")
            return 0

        for msg in messages:
            print(
                f"[{msg.topic}:{msg.partition}@{msg.offset}] "
                f"key={msg.key} value={msg.value_str}"
            )

        consumer.commit()
        print(f"\n{len(messages)} message(s) consumed and committed.")
        return 0

    def _metrics(self, args) -> int:
        stats = self.admin.broker_stats()
        print(format_json(stats))
        return 0

    def _dlq_list(self, args) -> int:
        print(f"DLQ for topic {args.topic!r}: (no entries)")
        return 0


def main():
    cli = CLI()
    sys.exit(cli.run())


if __name__ == "__main__":
    main()
