# rabbitMQ-common

A lightweight, type-safe RabbitMQ client for Node.js built on top of [`amqplib`](https://www.npmjs.com/package/amqplib).

## Features

- Simple `Producer` and `Consumer` classes — no boilerplate
- Automatic connection and channel management
- Queue assertion caching — asserts once, not on every publish
- Concurrent-safe initialisation — parallel callers share a single connection flight
- Retriable connections — a failed attempt clears state so the next call can retry
- Fully typed with TypeScript

## Installation

```bash
npm install rabbitmq-client
```

## Quick start

### Publishing a message

```typescript
import { Producer } from "rabbitmq-client";

const producer = new Producer("amqp://localhost");

await producer.publish("orders", { id: 1, item: "book" });

await producer.close();
```

### Consuming messages

```typescript
import { Consumer } from "rabbitmq-client";
import type { ConsumeMessage } from "amqplib";

class OrderConsumer extends Consumer<{ id: number; item: string }> {
  async onMessage(data: { id: number; item: string }, msg: ConsumeMessage) {
    console.log("Received order:", data);
  }
}

const consumer = new OrderConsumer("amqp://localhost");

await consumer.consume("orders");
```

## API

### `Producer`

#### `publish<T>(queue, message, options?)`

Sends a message to the given queue. Connects and asserts the queue on first use; subsequent calls to the same queue skip the assertion round-trip.

| Parameter            | Type      | Default | Description                                                     |
| -------------------- | --------- | ------- | --------------------------------------------------------------- |
| `queue`              | `string`  | —       | Queue name                                                      |
| `message`            | `T`       | —       | Any JSON-serialisable value                                     |
| `options.durable`    | `boolean` | `true`  | Queue survives broker restart. Applied only on first assertion. |
| `options.persistent` | `boolean` | `true`  | Message survives broker restart.                                |

Returns `Promise<boolean>`. The boolean reflects whether the socket write buffer is full (`false` = back-pressure; drain the channel before sending more).

```typescript
const ok = await producer.publish("jobs", { type: "email" });
if (!ok) {
  // socket buffer full — wait for the channel's "drain" event
}
```

---

### `Consumer<T>`

Abstract class. Extend it and implement `onMessage`.

#### `onMessage(data, originalMsg)` _(abstract)_

Called for every incoming message. Throw to nack and discard; return normally to ack.

| Parameter     | Type             | Description                                     |
| ------------- | ---------------- | ----------------------------------------------- |
| `data`        | `T`              | Parsed message payload                          |
| `originalMsg` | `ConsumeMessage` | Raw amqplib message (headers, properties, etc.) |

#### `consume(queue, options?)`

Starts consuming from the given queue.

| Parameter          | Type      | Default | Description                                                      |
| ------------------ | --------- | ------- | ---------------------------------------------------------------- |
| `queue`            | `string`  | —       | Queue name                                                       |
| `options.durable`  | `boolean` | `true`  | Queue survives broker restart                                    |
| `options.prefetch` | `number`  | `1`     | Max unacknowledged messages in flight. Set to `0` for unlimited. |

> **Note on `prefetch`:** The default is `1`, meaning the consumer processes one message at a time before fetching the next. This is the correct default for most worker patterns. If you need higher throughput and your handler is safe to run concurrently, increase this value explicitly. To remove the limit entirely, pass `prefetch: 0`.

---

### `close()`

Available on both `Producer` and `Consumer`. Gracefully closes the channel and connection and clears all internal state. Safe to call multiple times.

```typescript
await producer.close();
await consumer.close();
```

## Error handling

Errors thrown inside `onMessage` are caught automatically. The message is nack'd and discarded (not requeued):

```typescript
class MyConsumer extends Consumer<Job> {
  async onMessage(data: Job) {
    if (!data.id) {
      throw new Error("bad message"); // nack + discard, consumer keeps running
    }
    await process(data); // ack on success
  }
}
```

If you need dead-letter routing, configure a dead-letter exchange on the queue at the broker level.

## Connection lifecycle

Connections are lazy — nothing opens until the first `publish` or `consume` call. If the connection is lost, the internal state is cleared and the next call will reconnect automatically. If the initial connection attempt fails, it is safe to retry:

```typescript
// this will attempt a fresh connection each time until one succeeds
while (true) {
  try {
    await producer.publish("queue", data);
    break;
  } catch {
    await sleep(1000);
  }
}
```

## Requirements

- Node.js 18+
- `amqplib` peer dependency

## License

MIT
