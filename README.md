# rabbitmq-common v2

A lightweight, type-safe RabbitMQ client for Node.js built on top of [amqplib](https://www.npmjs.com/package/amqplib).

`rabbitmq-common v2` is a major rewrite focused on production reliability, automatic recovery, and safer RabbitMQ workflows.

While v1 provided a minimal abstraction over `amqplib`, real-world production workloads exposed several operational limitations:

- consumers stopped permanently after broker restarts
- failed messages could be lost
- reconnection handling had to be implemented manually
- dead-letter queue setup required boilerplate
- connection management was decentralized
- queue assertions were duplicated across producer instances

v2 solves these problems by introducing:

- automatic consumer recovery
- centralized connection management
- exponential retry reconnects
- built-in DLQ support
- safer nack/requeue behavior
- improved queue assertion caching
- consumer error lifecycle hooks

> ⚠️ v2 contains breaking changes.
> Please read the migration guide before upgrading from v1.

Looking for v1 documentation?
See [README_V1.md](./README_V1.md)

---

# Features

- Simple `Producer` and `Consumer` abstractions
- Automatic connection and channel management
- Shared singleton connection management via `ConnectionManager`
- Automatic reconnection with exponential backoff
- Consumer auto-recovery on channel or connection loss
- Built-in Dead Letter Queue (DLQ) support
- Queue assertion caching for producers
- Safer default message failure behavior
- Consumer lifecycle error hooks
- Fully typed with TypeScript
- Minimal boilerplate

---

# Why v2?

v1 worked well for simple RabbitMQ workflows, but production environments introduced challenges that applications had to solve manually:

- recovering consumers after RabbitMQ outages
- retrying failed connections
- handling poison messages safely
- implementing dead-letter queues
- preventing silent consumer death
- avoiding duplicated queue assertions

v2 moves these concerns into the library itself so applications remain simpler, safer, and more fault tolerant.

---

# Installation

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

# What's New in v2

## Automatic Consumer Recovery

Consumers now recover automatically after:

- broker restarts
- connection drops
- channel closures
- temporary RabbitMQ outages

Recovery includes:

- reconnecting
- recreating channels
- resubscribing consumers

No manual restart logic is required.

---

## ConnectionManager

v2 introduces centralized connection management.

```ts
import { ConnectionManager } from "rabbitmq-common";

await ConnectionManager.getConnection("amqp://localhost", -1);
```

### Benefits

- shared singleton connections
- avoids duplicate broker connections
- centralized retry handling
- safer reconnect behavior

---

## Exponential Retry Strategy

Failed connections automatically retry using exponential backoff.

Example retry delays:

```txt
2s → 4s → 8s → 16s → 30s max
```

Use infinite retries:

```ts
await ConnectionManager.getConnection(url, -1);
```

---

## Built-in Dead Letter Queue Support

Enable DLQ support directly from the consumer.

```ts
await consumer.consume("payments", {
  useDLQ: true,
});
```

When enabled:

- a dead-letter exchange is created automatically
- failed messages are routed safely
- poison messages no longer disappear silently

### Automatically Created Resources

| Resource | Naming Convention |
| -------- | ----------------- |
| DLX      | `${queue}_dlx`    |
| DLQ      | `${queue}_failed` |

---

## Consumer Error Lifecycle Hook

v2 introduces `onError()`.

```ts
async onError(error, data, originalMsg) {
  console.error(error);
}
```

Useful for:

- logging
- monitoring
- Sentry integration
- failed payload inspection
- metrics systems

### Example

```ts
class EmailConsumer extends Consumer<EmailJob> {
  async onMessage(data: EmailJob) {
    throw new Error("SMTP unavailable");
  }

  async onError(error, data) {
    console.log("Failed payload:", data);
  }
}
```

---

## Improved Queue Assertion Caching

### v1

Queue assertions were cached per producer instance.

This caused duplicated broker assertions across multiple producers.

---

### v2

Queue assertions are cached globally per process:

```ts
private static assertedQueues = new Set<string>();
```

Queues are asserted only once.

---

# API

# `Producer`

## `publish<T>(queue, message)`

Publishes a persistent message to a durable queue.

Queues are automatically asserted and cached internally.

---

## Parameters

| Parameter | Type     | Description               |
| --------- | -------- | ------------------------- |
| `queue`   | `string` | Queue name                |
| `message` | `T`      | JSON-serializable payload |

---

## Example

```ts
await producer.publish("jobs", {
  type: "email",
  userId: 1,
});
```

---

## Returns

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

If the method throws:

- `onError()` is triggered
- the message is nack'd automatically

---

## Parameters

| Parameter     | Type             | Description            |
| ------------- | ---------------- | ---------------------- |
| `data`        | `T`              | Parsed message payload |
| `originalMsg` | `ConsumeMessage` | Raw RabbitMQ message   |

---

# `onError(error, data?, originalMsg?)`

Optional lifecycle hook for consumer failures.

### Parameters

| Parameter     | Type                          | Description          |
| ------------- | ----------------------------- | -------------------- |
| `error`       | `Error`                       | Processing error     |
| `data`        | `T \| undefined`              | Parsed payload       |
| `originalMsg` | `ConsumeMessage \| undefined` | Raw RabbitMQ message |

---

# `consume(queue, options?)`

Starts consuming messages from a queue.

---

## Options

| Option     | Type      | Default | Description                     |
| ---------- | --------- | ------- | ------------------------------- |
| `prefetch` | `number`  | `1`     | Maximum unacknowledged messages |
| `useDLQ`   | `boolean` | `false` | Enables automatic DLQ setup     |

---

# Error Handling

Errors thrown inside `onMessage` are automatically handled.

Behavior depends on DLQ configuration:

| DLQ Enabled | Behavior                 |
| ----------- | ------------------------ |
| `false`     | Message is requeued      |
| `true`      | Message is routed to DLQ |

---

## Example

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

# Migration Guide

# Producer Changes

## v1

```ts
await producer.publish("jobs", payload, {
  durable: true,
  persistent: true,
});
```

---

## v2

```ts
await producer.publish("jobs", payload);
```

### Breaking Change

v2 simplifies producer behavior:

- queues are always durable
- messages are always persistent

The configuration options were removed intentionally to provide safer defaults.

---

# Consumer Changes

## v1

```ts
await consumer.consume("orders", {
  durable: true,
  prefetch: 1,
});
```

---

## v2

```ts
await consumer.consume("orders", {
  prefetch: 1,
  useDLQ: true,
});
```

---

## Breaking Change

`durable` is no longer configurable.

Queues are always durable in v2.

---

# Message Failure Behavior

## v1

Failed messages were discarded permanently:

```ts
channel.nack(msg, false, false);
```

This could lead to:

- lost jobs
- difficult debugging
- unrecoverable failures

---

## v2

Behavior is now safer:

| DLQ Enabled | Behavior                 |
| ----------- | ------------------------ |
| `false`     | Message is requeued      |
| `true`      | Message is routed to DLQ |

---

# Connection Handling

## v1

Applications had to implement retry logic manually.

Example:

```ts
while (true) {
  try {
    await producer.publish("queue", data);
    break;
  } catch {
    await sleep(1000);
  }
}
```

---

## v2

Automatic retry handling is built into the library.

No application-level reconnect loops required.

---

# Exports

```ts
import {
  Producer,
  Consumer,
  ConnectionManager,
  BaseRabbit,
} from "rabbitmq-common";
```

---

## Re-exported RabbitMQ Types

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
