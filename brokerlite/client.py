"""Python client library for BrokerLite.

Provides a high-level API for connecting to a BrokerLite server,
producing messages, consuming messages, and managing topics.
"""

from __future__ import annotations

import json
import socket
import struct
import threading
import time
from typing import Any, Callable, Optional

from .message import Message
from .protocol import (
    ApiKey, ErrorCode, Frame, HEADER_SIZE,
    encode_request, decode_response,
)
from .producer import RecordMetadata


class BrokerClient:
    """High-level client for connecting to a BrokerLite server.

    Usage:
        with BrokerClient("localhost", 9292) as client:
            client.create_topic("orders", partitions=4)
            client.produce("orders", b"order data", key="user-123")
            messages = client.consume("my-group", "orders")
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 9292,
        client_id: Optional[str] = None,
        timeout: float = 30.0,
    ):
        self.host = host
        self.port = port
        self.client_id = client_id or f"client-{id(self):x}"
        self.timeout = timeout

        self._socket: Optional[socket.socket] = None
        self._correlation_id = 0
        self._lock = threading.RLock()
        self._connected = False

    def connect(self) -> None:
        """Connect to the broker."""
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.settimeout(self.timeout)
        self._socket.connect((self.host, self.port))
        self._connected = True

    def close(self) -> None:
        """Close the connection."""
        self._connected = False
        if self._socket:
            try:
                self._socket.close()
            except OSError:
                pass
            self._socket = None

    def __enter__(self) -> BrokerClient:
        self.connect()
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def _next_correlation_id(self) -> int:
        with self._lock:
            self._correlation_id += 1
            return self._correlation_id

    def _send_request(self, api_key: ApiKey, data: dict) -> dict:
        """Send a request and wait for the response."""
        if not self._connected or not self._socket:
            raise ConnectionError("Not connected to broker")

        cid = self._next_correlation_id()
        frame_bytes = encode_request(api_key, cid, data)

        with self._lock:
            self._socket.sendall(frame_bytes)
            response_data = self._read_response()

        response = decode_response(response_data)

        if response.error_code != ErrorCode.NONE:
            error_msg = response.data.get("error", f"Error code: {response.error_code}")
            raise BrokerError(response.error_code, error_msg)

        return response.data

    def _read_response(self) -> bytes:
        """Read a complete response frame from the socket."""
        header = self._recv_exactly(4)
        frame_length = struct.unpack("!I", header)[0]
        body = self._recv_exactly(frame_length)
        return header + body

    def _recv_exactly(self, n: int) -> bytes:
        """Read exactly n bytes from the socket."""
        data = b""
        while len(data) < n:
            chunk = self._socket.recv(n - len(data))
            if not chunk:
                raise ConnectionError("Connection closed by broker")
            data += chunk
        return data

    # --- Topic operations ---

    def create_topic(self, name: str, partitions: int = 4) -> dict:
        """Create a new topic."""
        return self._send_request(ApiKey.CREATE_TOPICS, {
            "name": name,
            "partitions": partitions,
        })

    def delete_topic(self, name: str) -> dict:
        """Delete a topic."""
        return self._send_request(ApiKey.DELETE_TOPICS, {"name": name})

    def list_topics(self) -> dict:
        """List all topics and queues."""
        return self._send_request(ApiKey.METADATA, {})

    # --- Produce ---

    def produce(
        self,
        topic: str,
        value: bytes | str,
        key: Optional[str] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> dict:
        """Send a message to a topic."""
        if isinstance(value, bytes):
            value = value.decode("utf-8", errors="replace")
        return self._send_request(ApiKey.PRODUCE, {
            "topic": topic,
            "value": value,
            "key": key,
            "headers": headers or {},
        })

    # --- Consume ---

    def fetch(
        self,
        topic: str,
        partition: int = 0,
        offset: int = 0,
        max_messages: int = 100,
    ) -> list[dict]:
        """Fetch messages from a topic partition."""
        result = self._send_request(ApiKey.FETCH, {
            "topic": topic,
            "partition": partition,
            "offset": offset,
            "max_messages": max_messages,
        })
        return result.get("messages", [])

    def join_group(
        self,
        group_id: str,
        topic: str,
        consumer_id: Optional[str] = None,
    ) -> dict:
        """Join a consumer group for a topic."""
        return self._send_request(ApiKey.JOIN_GROUP, {
            "group_id": group_id,
            "topic": topic,
            "consumer_id": consumer_id,
        })

    def leave_group(self, group_id: str) -> dict:
        """Leave a consumer group."""
        return self._send_request(ApiKey.LEAVE_GROUP, {"group_id": group_id})

    def commit_offsets(self) -> dict:
        """Commit current consumer offsets."""
        return self._send_request(ApiKey.OFFSET_COMMIT, {})

    def fetch_offsets(self) -> dict:
        """Fetch committed offsets."""
        return self._send_request(ApiKey.OFFSET_FETCH, {})

    # --- Queue operations ---

    def create_queue(
        self,
        name: str,
        max_size: int = 0,
        priority: bool = False,
    ) -> dict:
        """Create a point-to-point queue."""
        return self._send_request(ApiKey.CREATE_QUEUE, {
            "name": name,
            "max_size": max_size,
            "priority": priority,
        })

    def enqueue(
        self,
        queue: str,
        value: bytes | str,
        key: Optional[str] = None,
        priority: int = 0,
    ) -> dict:
        """Enqueue a message to a queue."""
        if isinstance(value, bytes):
            value = value.decode("utf-8", errors="replace")
        return self._send_request(ApiKey.ENQUEUE, {
            "queue": queue,
            "value": value,
            "key": key,
            "priority": priority,
        })

    def dequeue(self, queue: str) -> Optional[dict]:
        """Dequeue a message from a queue."""
        result = self._send_request(ApiKey.DEQUEUE, {"queue": queue})
        return result.get("message")

    def ack(self, queue: str, message_id: str) -> dict:
        """Acknowledge a message from a queue."""
        return self._send_request(ApiKey.ACK, {
            "queue": queue,
            "message_id": message_id,
        })

    # --- Heartbeat & Metrics ---

    def heartbeat(self) -> dict:
        """Send a heartbeat."""
        return self._send_request(ApiKey.HEARTBEAT, {})

    def metrics(self) -> dict:
        """Get broker metrics."""
        return self._send_request(ApiKey.METRICS, {})

    # --- List groups ---

    def list_groups(self) -> list[str]:
        """List consumer groups."""
        result = self._send_request(ApiKey.LIST_GROUPS, {})
        return result.get("groups", [])

    def __repr__(self) -> str:
        status = "connected" if self._connected else "disconnected"
        return f"BrokerClient({self.host}:{self.port}, {status})"


class BrokerError(Exception):
    """Error returned by the broker."""

    def __init__(self, error_code: ErrorCode, message: str):
        self.error_code = error_code
        super().__init__(f"[{error_code.name}] {message}")
