"""TCP server — accepts client connections and handles protocol commands.

The BrokerServer listens on a TCP port and spawns a handler thread
per connection. Each handler reads protocol frames, dispatches them
to the broker, and sends back responses.
"""

from __future__ import annotations

import json
import logging
import socket
import threading
import time
from typing import Any, Optional

from .broker import Broker, BrokerConfig
from .consumer import Consumer, ConsumerConfig
from .message import Message
from .protocol import (
    ApiKey, ErrorCode, Frame, Request, Response,
    HEADER_SIZE,
)

logger = logging.getLogger(__name__)


class ClientHandler:
    """Handles a single client TCP connection.

    Reads frames from the socket, dispatches to the broker,
    and writes response frames back.
    """

    def __init__(
        self,
        conn: socket.socket,
        addr: tuple[str, int],
        broker: Broker,
        server: BrokerServer,
    ):
        self.conn = conn
        self.addr = addr
        self.broker = broker
        self.server = server
        self.client_id = f"{addr[0]}:{addr[1]}"

        self._consumer: Optional[Consumer] = None
        self._buffer = b""
        self._active = True

    def handle(self) -> None:
        """Main handler loop — read and process frames until disconnect."""
        logger.info("Client connected: %s", self.client_id)
        try:
            while self._active and self.server.is_running:
                try:
                    data = self.conn.recv(65536)
                except (ConnectionResetError, OSError):
                    break
                if not data:
                    break

                self._buffer += data
                self._process_buffer()
        except Exception as e:
            logger.error("Error handling client %s: %s", self.client_id, e)
        finally:
            self._cleanup()

    def _process_buffer(self) -> None:
        """Extract and process complete frames from the buffer."""
        while True:
            frame, self._buffer = Frame.read_from_buffer(self._buffer)
            if frame is None:
                break
            self._dispatch(frame)

    def _dispatch(self, frame: Frame) -> None:
        """Route a frame to the appropriate handler method."""
        try:
            payload = (
                json.loads(frame.payload.decode("utf-8"))
                if frame.payload else {}
            )
        except (json.JSONDecodeError, UnicodeDecodeError):
            self._send_error(frame, ErrorCode.INVALID_REQUEST, "Invalid payload")
            return

        handlers = {
            ApiKey.PRODUCE: self._handle_produce,
            ApiKey.FETCH: self._handle_fetch,
            ApiKey.METADATA: self._handle_metadata,
            ApiKey.CREATE_TOPICS: self._handle_create_topics,
            ApiKey.DELETE_TOPICS: self._handle_delete_topics,
            ApiKey.LIST_GROUPS: self._handle_list_groups,
            ApiKey.JOIN_GROUP: self._handle_join_group,
            ApiKey.LEAVE_GROUP: self._handle_leave_group,
            ApiKey.OFFSET_COMMIT: self._handle_offset_commit,
            ApiKey.OFFSET_FETCH: self._handle_offset_fetch,
            ApiKey.HEARTBEAT: self._handle_heartbeat,
            ApiKey.CREATE_QUEUE: self._handle_create_queue,
            ApiKey.ENQUEUE: self._handle_enqueue,
            ApiKey.DEQUEUE: self._handle_dequeue,
            ApiKey.ACK: self._handle_ack,
            ApiKey.METRICS: self._handle_metrics,
        }

        handler = handlers.get(frame.api_key)
        if handler:
            handler(frame, payload)
        else:
            self._send_error(
                frame, ErrorCode.INVALID_REQUEST,
                f"Unknown API key: {frame.api_key}"
            )

    def _handle_produce(self, frame: Frame, data: dict) -> None:
        topic = data.get("topic", "")
        if not topic:
            self._send_error(frame, ErrorCode.INVALID_REQUEST, "Missing topic")
            return

        msg = Message(
            topic=topic,
            value=data.get("value", "").encode("utf-8"),
            key=data.get("key"),
            headers=data.get("headers", {}),
        )

        try:
            metadata = self.broker.publish(msg)
            self._send_response(frame, ErrorCode.NONE, {
                "topic": metadata.topic,
                "partition": metadata.partition,
                "offset": metadata.offset,
                "message_id": metadata.message_id,
            })
        except ValueError as e:
            self._send_error(frame, ErrorCode.UNKNOWN_TOPIC, str(e))

    def _handle_fetch(self, frame: Frame, data: dict) -> None:
        topic_name = data.get("topic", "")
        partition_id = data.get("partition", 0)
        offset = data.get("offset", 0)
        max_messages = data.get("max_messages", 100)

        topic = self.broker.get_topic(topic_name)
        if not topic:
            self._send_error(frame, ErrorCode.UNKNOWN_TOPIC, f"Topic {topic_name!r} not found")
            return

        try:
            partition = topic.get_partition(partition_id)
            messages = partition.read(offset, max_messages)
            self._send_response(frame, ErrorCode.NONE, {
                "messages": [m.to_dict() for m in messages],
                "count": len(messages),
            })
        except ValueError as e:
            self._send_error(frame, ErrorCode.UNKNOWN_PARTITION, str(e))

    def _handle_metadata(self, frame: Frame, data: dict) -> None:
        topics = self.broker.list_topics()
        topic_info = {}
        for name in topics:
            topic = self.broker.get_topic(name)
            if topic:
                topic_info[name] = {
                    "partitions": topic.num_partitions,
                    "messages": topic.total_messages(),
                }
        self._send_response(frame, ErrorCode.NONE, {
            "topics": topic_info,
            "queues": self.broker.list_queues(),
        })

    def _handle_create_topics(self, frame: Frame, data: dict) -> None:
        name = data.get("name", "")
        partitions = data.get("partitions", 4)
        if not name:
            self._send_error(frame, ErrorCode.INVALID_REQUEST, "Missing topic name")
            return

        try:
            from .topic import TopicConfig
            config = TopicConfig(num_partitions=partitions)
            self.broker.create_topic(name, config)
            self._send_response(frame, ErrorCode.NONE, {"name": name, "created": True})
        except ValueError as e:
            self._send_error(frame, ErrorCode.TOPIC_ALREADY_EXISTS, str(e))

    def _handle_delete_topics(self, frame: Frame, data: dict) -> None:
        name = data.get("name", "")
        deleted = self.broker.delete_topic(name)
        self._send_response(frame, ErrorCode.NONE, {
            "name": name, "deleted": deleted,
        })

    def _handle_list_groups(self, frame: Frame, data: dict) -> None:
        groups = self.broker.list_consumer_groups()
        self._send_response(frame, ErrorCode.NONE, {"groups": groups})

    def _handle_join_group(self, frame: Frame, data: dict) -> None:
        group_id = data.get("group_id", "")
        topic = data.get("topic", "")
        consumer_id = data.get("consumer_id")

        consumer = Consumer(consumer_id=consumer_id)
        try:
            self.broker.subscribe(group_id, topic, consumer)
            self._consumer = consumer
            self._send_response(frame, ErrorCode.NONE, {
                "group_id": group_id,
                "consumer_id": consumer.consumer_id,
                "assigned_partitions": [
                    f"{p.topic}-{p.partition_id}"
                    for p in consumer.assigned_partitions
                ],
            })
        except ValueError as e:
            self._send_error(frame, ErrorCode.GROUP_NOT_FOUND, str(e))

    def _handle_leave_group(self, frame: Frame, data: dict) -> None:
        group_id = data.get("group_id", "")
        group = self.broker.get_consumer_group(group_id)
        if group and self._consumer:
            group.leave(self._consumer.consumer_id)
            self._consumer = None
        self._send_response(frame, ErrorCode.NONE, {"left": True})

    def _handle_offset_commit(self, frame: Frame, data: dict) -> None:
        if self._consumer:
            committed = self._consumer.commit()
            result = {f"{t}-{p}": o for (t, p), o in committed.items()}
            self._send_response(frame, ErrorCode.NONE, {"offsets": result})
        else:
            self._send_error(frame, ErrorCode.GROUP_NOT_FOUND, "Not in a group")

    def _handle_offset_fetch(self, frame: Frame, data: dict) -> None:
        if self._consumer:
            offsets = {}
            for p in self._consumer.assigned_partitions:
                key = f"{p.topic}-{p.partition_id}"
                offsets[key] = self._consumer.position(p.topic, p.partition_id)
            self._send_response(frame, ErrorCode.NONE, {"offsets": offsets})
        else:
            self._send_error(frame, ErrorCode.GROUP_NOT_FOUND, "Not in a group")

    def _handle_heartbeat(self, frame: Frame, data: dict) -> None:
        self._send_response(frame, ErrorCode.NONE, {"status": "alive"})

    def _handle_create_queue(self, frame: Frame, data: dict) -> None:
        name = data.get("name", "")
        max_size = data.get("max_size", 0)
        priority = data.get("priority", False)
        try:
            self.broker.create_queue(name, max_size=max_size, priority=priority)
            self._send_response(frame, ErrorCode.NONE, {"name": name, "created": True})
        except ValueError as e:
            self._send_error(frame, ErrorCode.INVALID_REQUEST, str(e))

    def _handle_enqueue(self, frame: Frame, data: dict) -> None:
        queue_name = data.get("queue", "")
        msg = Message(
            topic=queue_name,
            value=data.get("value", "").encode("utf-8"),
            key=data.get("key"),
            priority=data.get("priority", 0),
        )
        try:
            self.broker.enqueue(queue_name, msg)
            self._send_response(frame, ErrorCode.NONE, {"enqueued": True})
        except ValueError as e:
            self._send_error(frame, ErrorCode.QUEUE_NOT_FOUND, str(e))

    def _handle_dequeue(self, frame: Frame, data: dict) -> None:
        queue_name = data.get("queue", "")
        q = self.broker.get_queue(queue_name)
        if not q:
            self._send_error(frame, ErrorCode.QUEUE_NOT_FOUND, f"Queue {queue_name!r} not found")
            return
        msg = q.dequeue()
        if msg:
            self._send_response(frame, ErrorCode.NONE, {"message": msg.to_dict()})
        else:
            self._send_response(frame, ErrorCode.NONE, {"message": None})

    def _handle_ack(self, frame: Frame, data: dict) -> None:
        queue_name = data.get("queue", "")
        message_id = data.get("message_id", "")
        q = self.broker.get_queue(queue_name)
        if q:
            acked = q.acknowledge(message_id)
            self._send_response(frame, ErrorCode.NONE, {"acknowledged": acked})
        else:
            self._send_error(frame, ErrorCode.QUEUE_NOT_FOUND, "Queue not found")

    def _handle_metrics(self, frame: Frame, data: dict) -> None:
        snapshot = self.broker.snapshot()
        self._send_response(frame, ErrorCode.NONE, snapshot)

    def _send_response(
        self, request_frame: Frame, error_code: ErrorCode, data: dict
    ) -> None:
        resp = Response(
            correlation_id=request_frame.correlation_id,
            error_code=error_code,
            data=data,
        )
        frame_bytes = resp.to_frame(request_frame.api_key).to_bytes()
        try:
            self.conn.sendall(frame_bytes)
        except (BrokenPipeError, OSError):
            self._active = False

    def _send_error(
        self, request_frame: Frame, error_code: ErrorCode, message: str
    ) -> None:
        self._send_response(request_frame, error_code, {"error": message})

    def _cleanup(self) -> None:
        """Clean up resources on disconnect."""
        logger.info("Client disconnected: %s", self.client_id)
        if self._consumer:
            self._consumer.close()
        try:
            self.conn.close()
        except OSError:
            pass
        self.server._remove_handler(self)


class BrokerServer:
    """Multi-threaded TCP server for the message broker.

    Spawns one thread per client connection.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 9292,
        broker: Optional[Broker] = None,
        max_connections: int = 100,
    ):
        self.host = host
        self.port = port
        self.broker = broker or Broker()
        self.max_connections = max_connections

        self._socket: Optional[socket.socket] = None
        self._handlers: list[ClientHandler] = []
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()
        self._running = False

    @property
    def is_running(self) -> bool:
        return self._running

    def start(self, background: bool = True) -> None:
        """Start the TCP server.

        Args:
            background: If True, run in a daemon thread.
        """
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.settimeout(1.0)
        self._socket.bind((self.host, self.port))
        self._socket.listen(self.max_connections)
        self._running = True
        self.broker.start()

        logger.info("BrokerServer listening on %s:%d", self.host, self.port)

        if background:
            self._thread = threading.Thread(target=self._accept_loop, daemon=True)
            self._thread.start()
        else:
            self._accept_loop()

    def _accept_loop(self) -> None:
        """Accept incoming connections."""
        while self._running:
            try:
                conn, addr = self._socket.accept()
            except socket.timeout:
                continue
            except OSError:
                break

            with self._lock:
                if len(self._handlers) >= self.max_connections:
                    conn.close()
                    continue

            handler = ClientHandler(conn, addr, self.broker, self)
            with self._lock:
                self._handlers.append(handler)

            thread = threading.Thread(target=handler.handle, daemon=True)
            thread.start()

    def _remove_handler(self, handler: ClientHandler) -> None:
        with self._lock:
            if handler in self._handlers:
                self._handlers.remove(handler)

    def stop(self) -> None:
        """Stop the server and close all connections."""
        self._running = False
        self.broker.stop()

        if self._socket:
            try:
                self._socket.close()
            except OSError:
                pass

        with self._lock:
            for handler in self._handlers:
                handler._active = False
                try:
                    handler.conn.close()
                except OSError:
                    pass
            self._handlers.clear()

        if self._thread:
            self._thread.join(timeout=5)

        logger.info("BrokerServer stopped")

    @property
    def connection_count(self) -> int:
        with self._lock:
            return len(self._handlers)

    def __repr__(self) -> str:
        return (
            f"BrokerServer(host={self.host!r}, port={self.port}, "
            f"connections={self.connection_count})"
        )
