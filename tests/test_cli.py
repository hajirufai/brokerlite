"""Tests for the CLI tool."""

import pytest
from brokerlite.cli import CLI, format_table, format_json
from brokerlite.broker import Broker


class TestFormatTable:
    def test_basic_table(self):
        table = format_table(["Name", "Age"], [["Alice", "30"]])
        assert "Alice" in table
        assert "Name" in table

    def test_empty_rows(self):
        table = format_table(["A", "B"], [])
        assert "A" in table

    def test_wide_cells(self):
        table = format_table(["X"], [["VeryLongCellValue"]])
        assert "VeryLongCellValue" in table


class TestFormatJson:
    def test_basic(self):
        result = format_json({"key": "value"})
        assert '"key"' in result


class TestCLI:
    def setup_method(self):
        self.broker = Broker()
        self.cli = CLI(broker=self.broker)

    def test_topics_list_empty(self):
        assert self.cli.run(["topics", "list"]) == 0

    def test_topics_create(self):
        result = self.cli.run(["topics", "create", "--name", "orders"])
        assert result == 0
        assert "orders" in self.broker.list_topics()

    def test_topics_describe(self):
        self.cli.run(["topics", "create", "--name", "orders"])
        assert self.cli.run(["topics", "describe", "orders"]) == 0

    def test_topics_describe_nonexistent(self):
        assert self.cli.run(["topics", "describe", "nope"]) == 1

    def test_topics_delete(self):
        self.cli.run(["topics", "create", "--name", "temp"])
        assert self.cli.run(["topics", "delete", "temp"]) == 0

    def test_queues_create(self):
        assert self.cli.run(["queues", "create", "--name", "tasks"]) == 0

    def test_queues_list(self):
        self.cli.run(["queues", "create", "--name", "tasks"])
        assert self.cli.run(["queues", "list"]) == 0

    def test_produce(self):
        self.cli.run(["topics", "create", "--name", "orders"])
        result = self.cli.run([
            "produce", "orders", "--value", '{"event": "buy"}',
        ])
        assert result == 0

    def test_metrics(self):
        assert self.cli.run(["metrics"]) == 0

    def test_no_command(self):
        assert self.cli.run([]) == 1

    def test_groups_list_empty(self):
        assert self.cli.run(["groups", "list"]) == 0
