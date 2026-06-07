"""Tests for the TCP server and client."""

import time
import pytest
from brokerlite.server import BrokerServer
from brokerlite.client import BrokerClient, BrokerError
from brokerlite.broker import Broker
from brokerlite.topic import TopicConfig


@pytest.fixture
def server_port():
    """Find a free port."""
    import socket
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def broker_server(server_port):
    """Start a broker server for testing."""
    broker = Broker()
    server = BrokerServer(host="127.0.0.1", port=server_port, broker=broker)
    server.start(background=True)
    time.sleep(0.1)
    yield server
    server.stop()


@pytest.fixture
def client(broker_server, server_port):
    """Create a connected client."""
    c = BrokerClient(host="127.0.0.1", port=server_port, timeout=5.0)
    c.connect()
    yield c
    c.close()


class TestServerClient:
    def test_heartbeat(self, client):
        result = client.heartbeat()
        assert result["status"] == "alive"

    def test_create_topic(self, client):
        result = client.create_topic("orders", partitions=4)
        assert result["created"] is True
        assert result["name"] == "orders"

    def test_list_topics(self, client):
        client.create_topic("orders")
        result = client.list_topics()
        assert "orders" in result.get("topics", {})

    def test_produce_and_fetch(self, client):
        client.create_topic("orders", partitions=1)
        result = client.produce("orders", b"hello-world", key="k1")
        assert result["topic"] == "orders"
        assert result["offset"] >= 0

        messages = client.fetch("orders", partition=0, offset=0)
        assert len(messages) >= 1

    def test_produce_unknown_topic_auto_creates(self, client):
        """Publishing to an unknown topic auto-creates it."""
        result = client.produce("auto-topic", b"data")
        assert result["topic"] == "auto-topic"
        topics = client.list_topics()
        assert "auto-topic" in topics.get("topics", {})

    def test_delete_topic(self, client):
        client.create_topic("temp")
        result = client.delete_topic("temp")
        assert result["deleted"] is True

    def test_create_queue(self, client):
        result = client.create_queue("tasks")
        assert result["created"] is True

    def test_enqueue_dequeue(self, client):
        client.create_queue("tasks")
        client.enqueue("tasks", b"task-data")
        msg = client.dequeue("tasks")
        assert msg is not None

    def test_metrics(self, client):
        result = client.metrics()
        assert isinstance(result, dict)

    def test_multiple_messages(self, client):
        client.create_topic("events", partitions=1)
        for i in range(10):
            client.produce("events", f"event-{i}".encode())
        messages = client.fetch("events", partition=0, offset=0, max_messages=100)
        assert len(messages) == 10


class TestBrokerClient:
    def test_repr_disconnected(self):
        c = BrokerClient()
        assert "disconnected" in repr(c)

    def test_send_without_connect_raises(self):
        c = BrokerClient()
        with pytest.raises(ConnectionError):
            c.produce("t", b"v")


class TestBrokerError:
    def test_error_message(self):
        from brokerlite.protocol import ErrorCode
        err = BrokerError(ErrorCode.UNKNOWN_TOPIC, "Not found")
        assert "UNKNOWN_TOPIC" in str(err)
        assert "Not found" in str(err)
