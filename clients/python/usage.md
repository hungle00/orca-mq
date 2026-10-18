# Python Client Guide

This guide explains how to run OrcaMQ with Docker and use the Python client to publish and consume messages over TCP.

> **Note:** The Python client is currently provided as source code in this repository. It has not been published to PyPI, so you must copy the client source into your project before using it.

## 1. Start the OrcaMQ Server

### Prerequisites

- Docker
- Python 3.10+ (for the examples below)

Clone the repository:

```bash
git clone https://github.com/hungle00/orca-mq.git
cd orca-mq
```

Build the Docker image:

```bash
docker build -t orca-mq:latest .
```

Start the server:

```bash
docker run -d \
  --name orcamq_server \
  -p 5555:5555 \
  orca-mq:latest
```

Alternatively, use Docker Compose if the repository's Compose configuration is available:

```bash
docker compose up -d
```

Check the server logs:

```bash
docker logs -f orcamq_server
```

The server should report a startup message similar to:

```text
OrcaMQ server running on 0.0.0.0:5555
```

The broker is now listening on port `5555`.

## 2. Set Up the Python Client

The client source is included in the OrcaMQ repository. Copy the `OrcaMQClient` implementation into your Python project.

For example, organize your project like this:

```text
python-demo/
├── orcamq.py
├── publisher.py
└── subscriber.py
```

Place the client implementation in `orcamq.py`, making sure it exports `OrcaMQClient`.

No third-party OrcaMQ package installation is required. Install any dependencies explicitly imported by the client source, if applicable.

## 3. Publish Messages

Create `publisher.py`:

```python
from orcamq import OrcaMQClient

queue = OrcaMQClient(host="localhost", port=5555)

for i in range(20):
    queue.publish(
        stream="hello_stream", 
        payload={"message": f"Hello, OrcaMQ! {i}"}
    )

print("Published 20 messages")
```

Run the publisher:

```bash
python publisher.py
```

Each call to `publish()` sends a message to the `hello_stream` stream.

## 4. Subscribe to Messages

Create `subscriber.py`:

```python
from orcamq import OrcaMQClient

queue = OrcaMQClient(host="localhost", port=5555)

queue.subscribe(
    stream="hello_stream", 
    group="test_group", 
    callback_fn=lambda msg: print(f"[Subscriber] Received: {msg}")
)
```

Run the subscriber in a separate terminal:

```bash
python subscriber.py
```

The subscriber connects to OrcaMQ over TCP and consumes messages from `hello_stream` through the `test_group` consumer group.

Keep the subscriber running while publishing messages from another terminal.

> **Tip:** For a clean test, start the subscriber before running the publisher. Message delivery for messages published before a subscriber starts depends on the broker's consumer-group offset and delivery semantics.

## 5. Run Publisher and Subscriber Separately

A typical local setup uses three terminals:

| Terminal | Command | Purpose |
|---|---|---|
| 1 | `docker logs -f orcamq_server` | Monitor the broker |
| 2 | `python subscriber.py` | Consume messages |
| 3 | `python publisher.py` | Publish messages |

This demonstrates how separate Python processes communicate through OrcaMQ without connecting directly to each other.

## 6. Configuration

The client accepts a host and port:

```python
queue = OrcaMQClient(
    host="localhost",
    port=5555
)
```

- `host`: The hostname or IP address of the OrcaMQ server.
- `port`: The TCP port exposed by the server, `5555` in this example.

## Next Steps

- Explore consumer groups and run multiple consumers.
- Test publishing and consuming from separate processes.
- Integrate OrcaMQ with other languages through its TCP protocol.

