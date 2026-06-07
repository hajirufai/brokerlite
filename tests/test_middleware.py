"""Tests for message middleware."""

import pytest
from brokerlite.message import Message
from brokerlite.middleware import (
    MiddlewarePipeline,
    LoggingMiddleware,
    FilterMiddleware,
    TransformMiddleware,
    DeduplicationMiddleware,
    RoutingMiddleware,
    TimestampMiddleware,
)


class TestMiddlewarePipeline:
    def test_empty_pipeline(self):
        pipeline = MiddlewarePipeline()
        msg = Message(topic="t", value=b"v")
        result = pipeline.process(msg)
        assert result is not None
        assert result.value == b"v"

    def test_single_middleware(self):
        pipeline = MiddlewarePipeline()
        pipeline.add(LoggingMiddleware())
        msg = Message(topic="t", value=b"v")
        result = pipeline.process(msg)
        assert result is not None

    def test_filter_drops_message(self):
        pipeline = MiddlewarePipeline()
        pipeline.add(FilterMiddleware(lambda m: m.key == "drop"))
        msg = Message(topic="t", value=b"v", key="drop")
        assert pipeline.process(msg) is None

    def test_filter_passes_message(self):
        pipeline = MiddlewarePipeline()
        pipeline.add(FilterMiddleware(lambda m: m.key == "drop"))
        msg = Message(topic="t", value=b"v", key="keep")
        assert pipeline.process(msg) is not None

    def test_chaining_order(self):
        pipeline = MiddlewarePipeline()
        pipeline.add(TimestampMiddleware())
        pipeline.add(LoggingMiddleware())
        msg = Message(topic="t", value=b"v")
        result = pipeline.process(msg)
        assert result.headers.get("x-processed-at") is not None

    def test_remove_middleware(self):
        pipeline = MiddlewarePipeline()
        pipeline.add(LoggingMiddleware())
        assert pipeline.remove("LoggingMiddleware")
        assert len(pipeline) == 0

    def test_remove_nonexistent(self):
        pipeline = MiddlewarePipeline()
        assert not pipeline.remove("Nope")

    def test_middleware_names(self):
        pipeline = MiddlewarePipeline()
        pipeline.add(LoggingMiddleware())
        pipeline.add(TimestampMiddleware())
        assert pipeline.middleware_names == ["LoggingMiddleware", "TimestampMiddleware"]

    def test_clear(self):
        pipeline = MiddlewarePipeline()
        pipeline.add(LoggingMiddleware())
        pipeline.clear()
        assert len(pipeline) == 0


class TestLoggingMiddleware:
    def test_counts_messages(self):
        mw = LoggingMiddleware()
        for _ in range(5):
            mw.process(Message(topic="t", value=b"v"))
        assert mw.message_count == 5


class TestFilterMiddleware:
    def test_filter_count(self):
        mw = FilterMiddleware(lambda m: m.value == b"bad")
        mw.process(Message(topic="t", value=b"bad"))
        mw.process(Message(topic="t", value=b"good"))
        assert mw.filtered_count == 1


class TestTransformMiddleware:
    def test_transforms_message(self):
        def add_header(msg):
            msg.headers.set("x-transformed", "true")
            return msg

        mw = TransformMiddleware(add_header)
        msg = Message(topic="t", value=b"v")
        result = mw.process(msg)
        assert result.headers.get("x-transformed") == "true"
        assert mw.transformed_count == 1


class TestDeduplicationMiddleware:
    def test_deduplicates(self):
        mw = DeduplicationMiddleware()
        msg1 = Message(topic="t", value=b"v", id="dup-id")
        msg2 = Message(topic="t", value=b"v", id="dup-id")
        assert mw.process(msg1) is not None
        assert mw.process(msg2) is None
        assert mw.duplicate_count == 1

    def test_different_ids_pass(self):
        mw = DeduplicationMiddleware()
        msg1 = Message(topic="t", value=b"v", id="id-1")
        msg2 = Message(topic="t", value=b"v", id="id-2")
        assert mw.process(msg1) is not None
        assert mw.process(msg2) is not None

    def test_window_eviction(self):
        mw = DeduplicationMiddleware(window_size=3)
        for i in range(5):
            mw.process(Message(topic="t", value=b"v", id=f"msg-{i}"))
        # msg-0 and msg-1 should have been evicted
        old = Message(topic="t", value=b"v", id="msg-0")
        assert mw.process(old) is not None  # should pass again


class TestRoutingMiddleware:
    def test_routes_to_new_topic(self):
        mw = RoutingMiddleware(lambda m: "errors" if m.key == "err" else None)
        msg = Message(topic="events", value=b"v", key="err")
        result = mw.process(msg)
        assert result.topic == "errors"
        assert mw.routed_count == 1

    def test_no_route_keeps_original(self):
        mw = RoutingMiddleware(lambda m: None)
        msg = Message(topic="events", value=b"v")
        result = mw.process(msg)
        assert result.topic == "events"
        assert mw.routed_count == 0


class TestTimestampMiddleware:
    def test_adds_timestamp(self):
        mw = TimestampMiddleware()
        msg = Message(topic="t", value=b"v")
        result = mw.process(msg)
        assert result.headers.get("x-processed-at") is not None
