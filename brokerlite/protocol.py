"""Binary wire protocol for BrokerLite.

Defines the frame format, API keys, request/response encoding,
and error codes for client-server communication over TCP.

Frame format:
    [4 bytes: frame length][2 bytes: api_key][2 bytes: api_version]
    [4 bytes: correlation_id][N bytes: payload]
"""

from __future__ import annotations

import json
import struct
from dataclasses import dataclass
from enum import IntEnum
from typing import Any, Optional


class ApiKey(IntEnum):
    """Protocol API keys — each identifies a type of request."""
    PRODUCE = 0
    FETCH = 1
    LIST_OFFSETS = 2
    METADATA = 3
    OFFSET_COMMIT = 4
    OFFSET_FETCH = 5
    CREATE_TOPICS = 6
    DELETE_TOPICS = 7
    HEARTBEAT = 8
    JOIN_GROUP = 9
    LEAVE_GROUP = 10
    SYNC_GROUP = 11
    LIST_GROUPS = 12
    CREATE_QUEUE = 13
    ENQUEUE = 14
    DEQUEUE = 15
    ACK = 16
    NACK = 17
    METRICS = 18


class ErrorCode(IntEnum):
    """Protocol error codes."""
    NONE = 0
    UNKNOWN = -1
    UNKNOWN_TOPIC = 3
    INVALID_MESSAGE = 4
    UNKNOWN_PARTITION = 6
    OFFSET_OUT_OF_RANGE = 1
    GROUP_NOT_FOUND = 15
    QUEUE_FULL = 50
    QUEUE_NOT_FOUND = 51
    MESSAGE_TOO_LARGE = 10
    TOPIC_ALREADY_EXISTS = 36
    INVALID_REQUEST = 42
    BROKER_NOT_AVAILABLE = 8


# Header: frame_length(4) + api_key(2) + api_version(2) + correlation_id(4) = 12 bytes
HEADER_FORMAT = "!IHHi"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)


@dataclass
class Frame:
    """A protocol frame — the envelope for all messages on the wire."""
    api_key: ApiKey
    api_version: int
    correlation_id: int
    payload: bytes

    def to_bytes(self) -> bytes:
        """Serialize the frame to bytes for transmission."""
        payload_bytes = self.payload
        frame_length = HEADER_SIZE - 4 + len(payload_bytes)  # exclude length field itself
        header = struct.pack(
            HEADER_FORMAT,
            frame_length,
            self.api_key.value,
            self.api_version,
            self.correlation_id,
        )
        return header + payload_bytes

    @classmethod
    def from_bytes(cls, data: bytes) -> Frame:
        """Deserialize a frame from bytes."""
        if len(data) < HEADER_SIZE:
            raise ValueError(
                f"Frame too short: {len(data)} bytes (need {HEADER_SIZE})"
            )
        frame_length, api_key, api_version, correlation_id = struct.unpack(
            HEADER_FORMAT, data[:HEADER_SIZE]
        )
        payload = data[HEADER_SIZE:]
        return cls(
            api_key=ApiKey(api_key),
            api_version=api_version,
            correlation_id=correlation_id,
            payload=payload,
        )

    @classmethod
    def read_from_buffer(cls, buffer: bytes) -> tuple[Optional[Frame], bytes]:
        """Try to read a complete frame from a buffer.

        Returns (frame, remaining_buffer) or (None, buffer) if incomplete.
        """
        if len(buffer) < 4:
            return None, buffer

        frame_length = struct.unpack("!I", buffer[:4])[0]
        total_length = 4 + frame_length

        if len(buffer) < total_length:
            return None, buffer

        frame_data = buffer[:total_length]
        remaining = buffer[total_length:]
        return cls.from_bytes(frame_data), remaining


@dataclass
class Request:
    """A client request."""
    api_key: ApiKey
    api_version: int = 1
    correlation_id: int = 0
    data: dict[str, Any] = None

    def __post_init__(self):
        if self.data is None:
            self.data = {}

    def to_frame(self) -> Frame:
        payload = json.dumps(self.data).encode("utf-8")
        return Frame(
            api_key=self.api_key,
            api_version=self.api_version,
            correlation_id=self.correlation_id,
            payload=payload,
        )


@dataclass
class Response:
    """A server response."""
    correlation_id: int
    error_code: ErrorCode = ErrorCode.NONE
    data: dict[str, Any] = None

    def __post_init__(self):
        if self.data is None:
            self.data = {}

    def to_frame(self, api_key: ApiKey = ApiKey.METADATA) -> Frame:
        response_data = {
            "error_code": self.error_code.value,
            "data": self.data,
        }
        payload = json.dumps(response_data).encode("utf-8")
        return Frame(
            api_key=api_key,
            api_version=1,
            correlation_id=self.correlation_id,
            payload=payload,
        )


def encode_request(
    api_key: ApiKey,
    correlation_id: int,
    data: dict[str, Any],
    api_version: int = 1,
) -> bytes:
    """Encode a request into wire bytes."""
    req = Request(
        api_key=api_key,
        api_version=api_version,
        correlation_id=correlation_id,
        data=data,
    )
    return req.to_frame().to_bytes()


def decode_request(data: bytes) -> Request:
    """Decode wire bytes into a Request."""
    frame = Frame.from_bytes(data)
    payload = json.loads(frame.payload.decode("utf-8")) if frame.payload else {}
    return Request(
        api_key=frame.api_key,
        api_version=frame.api_version,
        correlation_id=frame.correlation_id,
        data=payload,
    )


def encode_response(
    correlation_id: int,
    error_code: ErrorCode,
    data: dict[str, Any],
    api_key: ApiKey = ApiKey.METADATA,
) -> bytes:
    """Encode a response into wire bytes."""
    resp = Response(
        correlation_id=correlation_id,
        error_code=error_code,
        data=data,
    )
    return resp.to_frame(api_key).to_bytes()


def decode_response(data: bytes) -> Response:
    """Decode wire bytes into a Response."""
    frame = Frame.from_bytes(data)
    payload = json.loads(frame.payload.decode("utf-8")) if frame.payload else {}
    return Response(
        correlation_id=frame.correlation_id,
        error_code=ErrorCode(payload.get("error_code", 0)),
        data=payload.get("data", {}),
    )
