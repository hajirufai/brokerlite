"""Tests for the binary wire protocol."""

import pytest
from brokerlite.protocol import (
    ApiKey, ErrorCode, Frame, Request, Response,
    encode_request, decode_request, encode_response, decode_response,
    HEADER_SIZE,
)


class TestFrame:
    def test_roundtrip(self):
        frame = Frame(
            api_key=ApiKey.PRODUCE,
            api_version=1,
            correlation_id=42,
            payload=b'{"topic": "orders"}',
        )
        raw = frame.to_bytes()
        parsed = Frame.from_bytes(raw)
        assert parsed.api_key == ApiKey.PRODUCE
        assert parsed.correlation_id == 42
        assert parsed.payload == b'{"topic": "orders"}'

    def test_too_short_raises(self):
        with pytest.raises(ValueError, match="too short"):
            Frame.from_bytes(b"\x00\x01")

    def test_read_from_buffer_incomplete(self):
        frame, remaining = Frame.read_from_buffer(b"\x00")
        assert frame is None
        assert remaining == b"\x00"

    def test_read_from_buffer_complete(self):
        original = Frame(
            api_key=ApiKey.FETCH,
            api_version=1,
            correlation_id=1,
            payload=b"test",
        )
        data = original.to_bytes()
        frame, remaining = Frame.read_from_buffer(data)
        assert frame is not None
        assert frame.api_key == ApiKey.FETCH
        assert remaining == b""

    def test_read_from_buffer_with_extra_data(self):
        original = Frame(
            api_key=ApiKey.METADATA,
            api_version=1,
            correlation_id=5,
            payload=b"{}",
        )
        data = original.to_bytes() + b"EXTRA"
        frame, remaining = Frame.read_from_buffer(data)
        assert frame is not None
        assert remaining == b"EXTRA"


class TestRequest:
    def test_to_frame_and_back(self):
        req = Request(
            api_key=ApiKey.PRODUCE,
            correlation_id=10,
            data={"topic": "orders", "value": "hello"},
        )
        raw = req.to_frame().to_bytes()
        parsed = decode_request(raw)
        assert parsed.api_key == ApiKey.PRODUCE
        assert parsed.data["topic"] == "orders"

    def test_default_data(self):
        req = Request(api_key=ApiKey.HEARTBEAT)
        assert req.data == {}


class TestResponse:
    def test_to_frame_and_back(self):
        resp = Response(
            correlation_id=10,
            error_code=ErrorCode.NONE,
            data={"offset": 42},
        )
        raw = resp.to_frame(ApiKey.PRODUCE).to_bytes()
        parsed = decode_response(raw)
        assert parsed.correlation_id == 10
        assert parsed.error_code == ErrorCode.NONE
        assert parsed.data["offset"] == 42

    def test_error_response(self):
        resp = Response(
            correlation_id=5,
            error_code=ErrorCode.UNKNOWN_TOPIC,
            data={"error": "Topic not found"},
        )
        raw = resp.to_frame().to_bytes()
        parsed = decode_response(raw)
        assert parsed.error_code == ErrorCode.UNKNOWN_TOPIC


class TestEncodeDecode:
    def test_encode_decode_request(self):
        raw = encode_request(
            ApiKey.CREATE_TOPICS, 99,
            {"name": "events", "partitions": 4},
        )
        req = decode_request(raw)
        assert req.api_key == ApiKey.CREATE_TOPICS
        assert req.correlation_id == 99
        assert req.data["name"] == "events"

    def test_encode_decode_response(self):
        raw = encode_response(
            99, ErrorCode.NONE, {"created": True},
        )
        resp = decode_response(raw)
        assert resp.correlation_id == 99
        assert resp.data["created"] is True


class TestApiKey:
    def test_all_keys_unique(self):
        values = [k.value for k in ApiKey]
        assert len(values) == len(set(values))

    def test_produce_is_zero(self):
        assert ApiKey.PRODUCE.value == 0


class TestErrorCode:
    def test_none_is_zero(self):
        assert ErrorCode.NONE.value == 0

    def test_unknown_is_negative(self):
        assert ErrorCode.UNKNOWN.value == -1
