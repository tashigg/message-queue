---
name: foxmq-mqtt-broker
description: FoxMQ is a decentralized MQTT 5.0 broker with consensus-based message ordering. Use when setting up distributed message queues, implementing producer-consumer patterns with guaranteed fair ordering, or building agent communication systems that require identical state across all nodes. Supports native binary installation with simple queue configurations.
---

# FoxMQ

Decentralized MQTT 5.0 broker with Vertex consensus for fair message ordering across distributed systems.

## When to Apply

- Setting up producer-consumer message queues with guaranteed ordering
- Building agent swarms requiring identical state across all nodes
- Migrating from centralized MQTT brokers to fault-tolerant clusters
- Implementing fair task bidding or distributed coordination systems

## Critical Rules

**Address Book Synchronization**: `address-book.toml` must be identical across ALL nodes

```bash
# WRONG - different address books on nodes
node1: ./foxmq address-book from-range 127.0.0.1 19793 19793
node2: ./foxmq address-book from-range 127.0.0.1 19794 19794

# RIGHT - identical address book on all nodes
./foxmq address-book from-list 172.18.0.2:19793 172.18.0.3:19793 172.18.0.4:19793
```

**Key File Security**: Each node needs unique private key, never share `key_N.pem` files

```bash
# WRONG - sharing same key file
./foxmq run --secret-key-file=foxmq.d/key_0.pem  # on all nodes

# RIGHT - unique key per node
./foxmq run --secret-key-file=foxmq.d/key_0.pem  # node 0
./foxmq run --secret-key-file=foxmq.d/key_1.pem  # node 1
```

**QoS Selection**: Use QoS 2 for exactly-once delivery in consensus scenarios

```python
# WRONG - QoS 0 for critical operations
client.publish("swarm/bid/task_1", bid_data, qos=0)

# RIGHT - QoS 2 for exactly-once consensus
client.publish("swarm/bid/task_1", bid_data, qos=2)
```

## Installation & Setup

### Binary Installation

```bash
# Linux
curl -LO https://github.com/tashigit/foxmq/releases/download/v0.3.1/foxmq_0.3.1_linux-amd64.zip
unzip foxmq_0.3.1_linux-amd64.zip && chmod +x foxmq

# macOS
curl -LO https://github.com/tashigit/foxmq/releases/download/v0.3.1/foxmq_0.3.1_macos-universal.zip
unzip foxmq_0.3.1_macos-universal.zip && chmod +x foxmq
```

### Single Node Setup

```bash
./foxmq address-book from-range 127.0.0.1 19793 19793
./foxmq user add
./foxmq run --secret-key-file=foxmq.d/key_0.pem
```

### Multi-Node Cluster

```bash
# Generate shared address book
./foxmq address-book from-list 172.18.0.2:19793 172.18.0.3:19793 172.18.0.4:19793

# Add users (sync users.toml across nodes)
./foxmq user add

# Run each node with unique key
./foxmq run --secret-key-file=foxmq.d/key_0.pem  # node 0
./foxmq run --secret-key-file=foxmq.d/key_1.pem  # node 1
```

## Producer-Consumer Patterns

### Basic Python Client

```python
import paho.mqtt.client as mqtt
import json, time

def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        client.subscribe("tasks/#", qos=1)

def on_message(client, userdata, msg):
    print(f"[{msg.topic}] {msg.payload.decode()}")

client = mqtt.Client(protocol=mqtt.MQTTv5)
client.username_pw_set("producer", "password")
client.on_connect = on_connect
client.on_message = on_message
client.connect("127.0.0.1", 1883, keepalive=60)
```

### Publishing Messages

```python
# Simple queue message
task = {"id": "task_001", "data": "process_data", "timestamp": int(time.time() * 1000)}
client.publish("tasks/queue", json.dumps(task), qos=1)

# Retained state message
state = {"worker_id": "worker_1", "status": "available"}
client.publish("workers/state/worker_1", json.dumps(state), qos=1, retain=True)

# Clear retained message
client.publish("workers/state/worker_1", b"", qos=1, retain=True)
```

### Topic Wildcards

```python
client.subscribe("tasks/+/status", qos=1)   # single level
client.subscribe("workers/#", qos=1)        # multi level
```

## Debugging & Troubleshooting

### Enable JSON Logging

```bash
./foxmq run --secret-key-file=foxmq.d/key_0.pem --log=json
```

### Rust Log Levels

```bash
RUST_LOG=warn ./foxmq run --secret-key-file=foxmq.d/key_0.pem
RUST_LOG=debug ./foxmq run --secret-key-file=foxmq.d/key_0.pem
```

### Last Will Testament

```python
client = mqtt.Client(protocol=mqtt.MQTTv5)
client.will_set(
    topic="workers/state/worker_1",
    payload=json.dumps({"status": "offline"}),
    qos=1,
    retain=True
)
```

## Configuration Files

Located in `foxmq.d/` directory:

- **address-book.toml**: Network map (identical on all nodes)
- **key_N.pem**: Private keys (unique per node)
- **users.toml**: Argon2id hashed credentials (sync across nodes)

## Common Mistakes

- **Mismatched address books**: Consensus fails if nodes have different network maps
- **Shared private keys**: Each node needs unique key file for security
- **Wrong QoS levels**: Use QoS 2 for exactly-once consensus operations
- **Missing authentication**: Always configure users before production deployment
- **Cluster sizing**: Minimum 4 nodes recommended for production (tolerates 1 failure)