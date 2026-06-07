# BrokerLite

An in-memory message broker built from scratch in Python. Zero external dependencies — only the standard library.

Implements the core primitives of systems like Apache Kafka and RabbitMQ: partitioned topics, consumer groups, point-to-point queues, write-ahead logs, acknowledgments, dead letter queues, retry policies, backpressure, schema validation, middleware pipelines, a binary wire protocol, and a TCP server/client.

## Features

| Category | What's included |
|----------|----------------|
| **Pub/Sub** | Partitioned topics, key-based routing, consumer groups with range/round-robin assignment |
| **Queues** | FIFO and priority queues, visibility timeouts, bounded capacity |
| **Reliability** | Acknowledgments, dead letter queues, configurable retry (fixed/exponential/jitter) |
| **Storage** | Write-ahead log with segment rotation, SQLite metadata, offset tracking |
| **Flow Control** | Token-bucket rate limiting, queue-depth backpressure, slow-consumer detection |
| **Serialization** | JSON + custom binary serializer, schema registry with backward compatibility checks |
| **Networking** | Length-prefixed binary protocol, TCP server with thread-per-connection, Python client |
| **Middleware** | Composable pipeline — logging, filtering, transforms, deduplication |
| **Operations** | Metrics collector (rates, percentiles), CLI admin tool, retention & compaction |

## Quick Start

```bash
git clone https://github.com/hajirufai/brokerlite.git
cd brokerlite
```

### Pub/Sub

```python
from brokerlite.broker import Broker
from brokerlite.consumer import Consumer, ConsumerConfig
from brokerlite.message import Message
from brokerlite.topic import TopicConfig

broker = Broker()
broker.start()
broker.create_topic("orders", TopicConfig(num_partitions=4))

# Publish
for i in range(10):
    msg = Message(topic="orders", value=f"order-{i}".encode(), key=f"user-{i % 3}")
    metadata = broker.publish(msg)
    print(f"offset={metadata.offset}, partition={metadata.partition}")

# Consume
consumer = Consumer(config=ConsumerConfig(group_id="processors"))
broker.subscribe("processors", "orders", consumer)
for msg in consumer.poll():
    print(f"{msg.key}: {msg.value_str}")
consumer.commit()
```

### Work Queue

```python
broker.create_queue("tasks", max_size=1000)
broker.enqueue("tasks", Message(topic="tasks", value=b"resize-image"))

q = broker.get_queue("tasks")
msg = q.dequeue()
# process...
q.acknowledge(msg.id)
```

### TCP Server

```python
from brokerlite.server import BrokerServer
from brokerlite.client import BrokerClient

# Server
server = BrokerServer(host="0.0.0.0", port=9092)
server.start(background=True)

# Client
client = BrokerClient(host="localhost", port=9092)
client.connect()
client.create_topic("events")
client.produce("events", b"hello", key="k1")
messages = client.fetch("events", partition=0, offset=0)
```

## Architecture

```
┌──────────────────────────────────────────────────┐
│                   BrokerLite                     │
├──────────────────────────────────────────────────┤
│  Middleware Pipeline (log, filter, transform)    │
├────────────────────┬─────────────────────────────┤
│   Topics           │   Queues                    │
│  ┌──────────────┐  │  ┌─────────────────┐        │
│  │ Partition 0  │  │  │ MessageQueue    │        │
│  │ Partition 1  │  │  │ PriorityQueue   │        │
│  │ Partition N  │  │  └─────────────────┘        │
│  └──────────────┘  │                             │
├────────────────────┴─────────────────────────────┤
│  Consumer Groups (range / round-robin)           │
├──────────────────────────────────────────────────┤
│  Ack Manager → Retry Policy → Dead Letter Queue  │
├──────────────────────────────────────────────────┤
│  Write-Ahead Log  │  Metrics  │  Backpressure    │
├──────────────────────────────────────────────────┤
│  Binary Protocol  │  TCP Server  │  CLI Admin    │
└──────────────────────────────────────────────────┘
```

## Project Structure

```
brokerlite/
├── message.py        # Message, MessageHeaders, MessageBatch
├── partition.py       # Append-only log with offset tracking
├── topic.py           # Partitioned topic with key-based routing
├── queue.py           # FIFO + priority queues
├── consumer.py        # Consumer, ConsumerGroup, assignment strategies
├── producer.py        # Batching producer with callbacks
├── broker.py          # Central routing engine
├── storage.py         # Write-ahead log + SQLite metadata
├── protocol.py        # Binary wire protocol
├── server.py          # TCP server
├── client.py          # Python client
├── ack.py             # Acknowledgment tracking
├── dlq.py             # Dead letter queue
├── retry.py           # Retry policies (fixed/exponential/jitter)
├── backpressure.py    # Rate limiting + flow control
├── serializer.py      # JSON/binary serializers + schema registry
├── metrics.py         # Rates, percentiles, lag tracking
├── middleware.py       # Composable message pipeline
├── admin.py           # Admin operations
├── cli.py             # Command-line interface
└── utils.py           # Hashing, ID generation, formatting

tests/                 # 315 tests across 16 modules
examples/              # 5 runnable examples
```

## Testing

```bash
pip install pytest
python -m pytest tests/ -v
```

315 tests, zero external dependencies.

## Examples

| Example | Description |
|---------|-------------|
| `basic_pubsub.py` | Publish/subscribe through partitioned topics |
| `work_queue.py` | Point-to-point task processing with DLQ |
| `consumer_groups.py` | Parallel consumption with partition rebalancing |
| `dead_letters.py` | Retry policies and dead letter handling |
| `dashboard_demo.py` | Metrics collection and broker introspection |

Run any example:

```bash
PYTHONPATH=. python examples/basic_pubsub.py
```

## License

MIT
