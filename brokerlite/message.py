"""Message model for BrokerLite.

A message is the fundamental unit of data flowing through the broker.
Each message has an ID, topic, optional key (for partitioning), value (payload),
headers (metadata), timestamps, and delivery tracking fields.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional


class MessageHeaders:
    """Key-value metadata attached to a message.

    Headers are used for routing, tracing, retry tracking, and custom metadata
    without modifying the message value.
    """

    def __init__(self, headers: Optional[dict[str, str]] = None):
        self._headers: dict[str, str] = dict(headers) if headers else {}

    def set(self, key: str, value: str) -> None:
        self._headers[key] = value

    def get(self, key: str, default: Optional[str] = None) -> Optional[str]:
        return self._headers.get(key, default)

    def remove(self, key: str) -> Optional[str]:
        return self._headers.pop(key, None)

    def has(self, key: str) -> bool:
        return key in self._headers

    def keys(self) -> list[str]:
        return list(self._headers.keys())

    def items(self) -> list[tuple[str, str]]:
        return list(self._headers.items())

    def to_dict(self) -> dict[str, str]:
        return dict(self._headers)

    @classmethod
    def from_dict(cls, data: dict[str, str]) -> MessageHeaders:
        return cls(data)

    def __len__(self) -> int:
        return len(self._headers)

    def __repr__(self) -> str:
        return f"MessageHeaders({self._headers})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, MessageHeaders):
            return self._headers == other._headers
        return NotImplemented


@dataclass
class Message:
    """A single message in the broker.

    Attributes:
        id: Unique message identifier (UUID).
        topic: Topic name this message belongs to.
        key: Optional partitioning key. Messages with the same key
             go to the same partition.
        value: The message payload as bytes.
        headers: Optional key-value metadata.
        timestamp: Producer-side creation time (epoch seconds).
        broker_timestamp: Broker-side receipt time (epoch seconds).
        partition: Assigned partition number (-1 = unassigned).
        offset: Position within the partition log (-1 = unassigned).
        ttl: Time-to-live in seconds (0 = no expiry).
        priority: Priority level 0-9 (0 = lowest, 9 = highest).
        producer_id: Identifier of the producing client.
        sequence_number: Producer sequence for idempotent delivery.
    """

    topic: str
    value: bytes
    key: Optional[str] = None
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    headers: MessageHeaders = field(default_factory=MessageHeaders)
    timestamp: float = field(default_factory=time.time)
    broker_timestamp: float = 0.0
    partition: int = -1
    offset: int = -1
    ttl: int = 0
    priority: int = 0
    producer_id: str = ""
    sequence_number: int = -1

    def __post_init__(self) -> None:
        if isinstance(self.value, str):
            self.value = self.value.encode("utf-8")
        if isinstance(self.headers, dict):
            self.headers = MessageHeaders(self.headers)

    @property
    def value_str(self) -> str:
        """Decode value as UTF-8 string."""
        return self.value.decode("utf-8")

    @property
    def size_bytes(self) -> int:
        """Approximate size of the message in bytes."""
        size = len(self.value)
        if self.key:
            size += len(self.key.encode("utf-8"))
        size += len(self.id.encode("utf-8"))
        size += len(self.topic.encode("utf-8"))
        for k, v in self.headers.items():
            size += len(k.encode("utf-8")) + len(v.encode("utf-8"))
        return size

    def is_expired(self) -> bool:
        """Check if the message has exceeded its TTL."""
        if self.ttl <= 0:
            return False
        return time.time() > self.timestamp + self.ttl

    def to_dict(self) -> dict[str, Any]:
        """Serialize message to a dictionary."""
        return {
            "id": self.id,
            "topic": self.topic,
            "key": self.key,
            "value": self.value.decode("utf-8", errors="replace"),
            "headers": self.headers.to_dict(),
            "timestamp": self.timestamp,
            "broker_timestamp": self.broker_timestamp,
            "partition": self.partition,
            "offset": self.offset,
            "ttl": self.ttl,
            "priority": self.priority,
            "producer_id": self.producer_id,
            "sequence_number": self.sequence_number,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Message:
        """Deserialize message from a dictionary."""
        headers = MessageHeaders.from_dict(data.get("headers", {}))
        value = data.get("value", "")
        if isinstance(value, str):
            value = value.encode("utf-8")
        return cls(
            id=data.get("id", str(uuid.uuid4())),
            topic=data["topic"],
            key=data.get("key"),
            value=value,
            headers=headers,
            timestamp=data.get("timestamp", time.time()),
            broker_timestamp=data.get("broker_timestamp", 0.0),
            partition=data.get("partition", -1),
            offset=data.get("offset", -1),
            ttl=data.get("ttl", 0),
            priority=data.get("priority", 0),
            producer_id=data.get("producer_id", ""),
            sequence_number=data.get("sequence_number", -1),
        )

    def to_bytes(self) -> bytes:
        """Serialize message to bytes for wire protocol."""
        return json.dumps(self.to_dict()).encode("utf-8")

    @classmethod
    def from_bytes(cls, data: bytes) -> Message:
        """Deserialize message from bytes."""
        return cls.from_dict(json.loads(data.decode("utf-8")))

    def clone(self, **overrides: Any) -> Message:
        """Create a copy of this message with optional field overrides."""
        d = self.to_dict()
        d.update(overrides)
        if "value" in overrides and isinstance(overrides["value"], bytes):
            d["value"] = overrides["value"].decode("utf-8", errors="replace")
        return Message.from_dict(d)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Message):
            return self.id == other.id
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.id)

    def __repr__(self) -> str:
        val_preview = self.value[:50].decode("utf-8", errors="replace")
        if len(self.value) > 50:
            val_preview += "..."
        return (
            f"Message(id={self.id[:8]}..., topic={self.topic!r}, "
            f"key={self.key!r}, value={val_preview!r}, "
            f"partition={self.partition}, offset={self.offset})"
        )


class MessageBatch:
    """A batch of messages for efficient transmission.

    Batching amortizes network overhead — multiple messages are sent
    in a single protocol frame.
    """

    def __init__(self, messages: Optional[list[Message]] = None):
        self._messages: list[Message] = list(messages) if messages else []

    def add(self, message: Message) -> None:
        self._messages.append(message)

    def extend(self, messages: list[Message]) -> None:
        self._messages.extend(messages)

    @property
    def messages(self) -> list[Message]:
        return list(self._messages)

    @property
    def size(self) -> int:
        return len(self._messages)

    @property
    def total_bytes(self) -> int:
        return sum(m.size_bytes for m in self._messages)

    def by_topic(self) -> dict[str, list[Message]]:
        """Group messages by topic."""
        groups: dict[str, list[Message]] = {}
        for msg in self._messages:
            groups.setdefault(msg.topic, []).append(msg)
        return groups

    def by_partition(self) -> dict[tuple[str, int], list[Message]]:
        """Group messages by (topic, partition)."""
        groups: dict[tuple[str, int], list[Message]] = {}
        for msg in self._messages:
            key = (msg.topic, msg.partition)
            groups.setdefault(key, []).append(msg)
        return groups

    def clear(self) -> None:
        self._messages.clear()

    def to_bytes(self) -> bytes:
        """Serialize batch to bytes."""
        data = [m.to_dict() for m in self._messages]
        return json.dumps(data).encode("utf-8")

    @classmethod
    def from_bytes(cls, data: bytes) -> MessageBatch:
        """Deserialize batch from bytes."""
        items = json.loads(data.decode("utf-8"))
        messages = [Message.from_dict(item) for item in items]
        return cls(messages)

    def __len__(self) -> int:
        return len(self._messages)

    def __iter__(self):
        return iter(self._messages)

    def __repr__(self) -> str:
        return f"MessageBatch(size={self.size}, bytes={self.total_bytes})"
