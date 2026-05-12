# rabbitmq-common

A lightweight, type-safe RabbitMQ client for Node.js built on top of [amqplib](https://www.npmjs.com/package/amqplib).

## Features

- Simple `Producer` and `Consumer` abstractions
- Automatic connection and channel management
- Shared singleton connection management via `ConnectionManager`
- Automatic reconnection with exponential backoff
- Consumer auto-recovery on channel or connection loss
- Built-in Dead Letter Queue (DLQ) support
- Queue assertion caching for producers
- Fully typed with TypeScript
- Minimal boilerplate

---

## Installation

```bash
npm install rabbitmq-common
```

---

# Quick Start

## Publishing Messages

```ts
import { Producer } from "rabbitmq-common";

const producer = new Producer("amqp://localhost");

await producer.publish("orders", {
  id: 1,
  item: "book",
});
```

---

## Consuming Messages

```ts
import { Consumer } from "rabbitmq-common";
import type { ConsumeMessage } from "rabbitmq-common";

class OrderConsumer extends Consumer<{ id: number; item: string }> {
  async onMessage(data: { id: number; item: string }, msg: ConsumeMessage) {
    console.log("Received order:", data);
  }
}

const consumer = new OrderConsumer("amqp://localhost");

await consumer.consume("orders");
```

---

# API

## `Producer`

### `publish<T>(queue, message)`

Publishes a persistent message to a durable queue.

Queues are asserted only once per process and cached internally.

### Parameters

| Parameter | Type     | Description               |
| --------- | -------- | ------------------------- |
| `queue`   | `string` | Queue name                |
| `message` | `T`      | JSON-serializable payload |

### Example

```ts
await producer.publish("jobs", {
  type: "email",
  userId: 1,
});
```

### Returns

```ts
Promise<boolean>;
```

The boolean indicates whether the message was successfully written to the socket buffer.

---

# `Consumer<T>`

Abstract class for consuming typed RabbitMQ messages.

You must extend the class and implement `onMessage`.

---

## `onMessage(data, originalMsg)`

Called whenever a message is received.

If the method throws, the message is automatically nack'd.

### Parameters

| Parameter     | Type             | Description            |
| ------------- | ---------------- | ---------------------- |
| `data`        | `T`              | Parsed message payload |
| `originalMsg` | `ConsumeMessage` | Raw RabbitMQ message   |

---

## `onError(error, data?, originalMsg?)`

Optional lifecycle hook for handling consumer errors.

Override this method to integrate logging systems, monitoring, or alerts.

### Default implementation

```ts
async onError(error: Error) {
  console.error(error.message);
}
```

### Example

```ts
class EmailConsumer extends Consumer<EmailJob> {
  async onMessage(data: EmailJob) {
    throw new Error("SMTP unavailable");
  }

  async onError(error: Error, data?: EmailJob) {
    console.log("Failed message:", data);
    console.error(error);
  }
}
```

---

## `consume(queue, options?)`

Starts consuming messages from a queue.

### Options

| Option     | Type      | Default | Description                               |
| ---------- | --------- | ------- | ----------------------------------------- |
| `prefetch` | `number`  | `1`     | Maximum unacknowledged messages           |
| `useDLQ`   | `boolean` | `false` | Enables automatic dead-letter queue setup |

---

## Dead Letter Queue (DLQ)

When `useDLQ: true` is enabled:

- A dead-letter exchange is automatically created
- Failed messages are routed to a failed queue
- Messages are not requeued infinitely

### Automatically created resources

| Resource | Name              |
| -------- | ----------------- |
| DLX      | `${queue}_dlx`    |
| DLQ      | `${queue}_failed` |

### Example

```ts
await consumer.consume("payments", {
  useDLQ: true,
});
```

If processing fails repeatedly, failed messages are routed to:

```txt
payments_failed
```

---

# Automatic Recovery

Consumers automatically recover from:

- connection loss
- channel closure
- broker restarts

Recovery includes:

- reconnecting
- recreating channels
- resubscribing consumers

A retry attempt is automatically triggered after a short delay.

---

# ConnectionManager

`ConnectionManager` provides shared connection management with retry support.

It maintains a singleton RabbitMQ connection internally.

---

## `ConnectionManager.getConnection(url, maxRetries)`

Returns a shared RabbitMQ connection.

### Parameters

| Parameter    | Type     | Description                                        |
| ------------ | -------- | -------------------------------------------------- |
| `url`        | `string` | RabbitMQ connection URL                            |
| `maxRetries` | `number` | Maximum retry attempts (`-1` for infinite retries) |

### Example

```ts
import { ConnectionManager } from "rabbitmq-common";

const connection = await ConnectionManager.getConnection(
  "amqp://localhost",
  -1,
);
```

---

## Retry Strategy

Failed connections retry automatically using exponential backoff.

Example retry delays:

```txt
2s → 4s → 8s → 16s → 30s max
```

---

# Error Handling

Errors thrown inside `onMessage` are automatically handled.

Behavior depends on DLQ configuration:

| DLQ Enabled | Behavior                 |
| ----------- | ------------------------ |
| `false`     | Message is requeued      |
| `true`      | Message is routed to DLQ |

Example:

```ts
class MyConsumer extends Consumer<Job> {
  async onMessage(data: Job) {
    if (!data.id) {
      throw new Error("Invalid payload");
    }
  }
}
```

---

# Exports

The package exports:

```ts
import {
  Producer,
  Consumer,
  ConnectionManager,
  BaseRabbit,
} from "rabbitmq-common";
```

It also re-exports RabbitMQ types:

```ts
import type { Channel, ChannelModel, ConsumeMessage } from "rabbitmq-common";
```

---

# Requirements

- Node.js 18+
- RabbitMQ server
- `amqplib`

---

# License

MIT
