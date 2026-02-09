# FoxMQ Usage Example

This example demonstrates how to run the FoxMQ broker and test it using standard MQTT clients in a WSL environment.

## Prerequisites

Ensure you are in a **WSL** terminal, as the Tashi Vertex native libraries require Linux for linking and execution.

## Step-by-Step Demo

### 1. Terminal 1: Start the Broker
Start the FoxMQ broker with anonymous login enabled for testing:

```bash
cd /mnt/e/projects/tashi/message-queue
cargo run -- run foxmq.d --allow-anonymous-login
```

### 2. Terminal 2: Subscribe to a Topic
Open a second WSL terminal and subscribe to a test topic. This client will wait for messages ordered by the Tashi Consensus Engine.

```bash
mosquitto_sub -h 127.0.0.1 -p 1883 -v -t "tashi/mesh/demo"
```

### 3. Terminal 3: Publish a Message
Open a third WSL terminal and send a message. This message is wrapped in a Tashi transaction, ordered by the consensus layer, and then delivered to all subscribers.

```bash
mosquitto_pub -h 127.0.0.1 -p 1883 -t "tashi/mesh/demo" -m "Message ordered by Tashi Consensus!"
```

## What's Happening?

1. **MQTT Publish**: The `mosquitto_pub` command sends the message to FoxMQ.
2. **Consensus Layer**: FoxMQ submits the message to the `tashi-vertex`, which assigns it a global order and a consensus timestamp.
3. **MQTT Delivery**: FoxMQ receives the ordered event back and delivers it to `mosquitto_sub`.

This ensures that in a multi-node cluster, every node sees the exact same sequence of messages at the exact same time.
