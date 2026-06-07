"""BrokerLite — An in-memory message broker built from scratch in Python."""

__version__ = "1.0.0"

from .message import Message, MessageBatch, MessageHeaders
from .topic import Topic, TopicConfig
from .partition import Partition
from .queue import MessageQueue, PriorityQueue
from .producer import Producer, ProducerConfig, RecordMetadata
from .consumer import Consumer, ConsumerGroup, ConsumerConfig
from .broker import Broker, BrokerConfig
from .storage import WriteAheadLog, LogSegment
from .protocol import (
    Frame, Request, Response, ApiKey, ErrorCode,
    encode_request, decode_request, encode_response, decode_response,
)
from .server import BrokerServer
from .client import BrokerClient
from .ack import AckMode, AckManager
from .dlq import DeadLetterQueue, DLQEntry
from .retry import RetryPolicy, RetryResult
from .backpressure import RateLimiter, BackpressureManager
from .serializer import JsonSerializer, BinarySerializer, SchemaRegistry
from .metrics import MetricsCollector, MetricsSnapshot
from .middleware import (
    Middleware, MiddlewarePipeline,
    LoggingMiddleware, FilterMiddleware, TransformMiddleware,
    DeduplicationMiddleware,
)
from .admin import AdminManager

__all__ = [
    "Message", "MessageBatch", "MessageHeaders",
    "Topic", "TopicConfig",
    "Partition",
    "MessageQueue", "PriorityQueue",
    "Producer", "ProducerConfig", "RecordMetadata",
    "Consumer", "ConsumerGroup", "ConsumerConfig",
    "Broker", "BrokerConfig",
    "WriteAheadLog", "LogSegment",
    "Frame", "Request", "Response", "ApiKey", "ErrorCode",
    "BrokerServer",
    "BrokerClient",
    "AckMode", "AckManager",
    "DeadLetterQueue", "DLQEntry",
    "RetryPolicy", "RetryResult",
    "RateLimiter", "BackpressureManager",
    "JsonSerializer", "BinarySerializer", "SchemaRegistry",
    "MetricsCollector", "MetricsSnapshot",
    "Middleware", "MiddlewarePipeline",
    "LoggingMiddleware", "FilterMiddleware", "TransformMiddleware",
    "DeduplicationMiddleware",
    "AdminManager",
]
