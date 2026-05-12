# rabbitmq-common

A lightweight, TypeScript-first RabbitMQ client built on top of [`amqplib`](https://www.npmjs.com/package/amqplib). It provides a shared connection manager, an abstract base channel handler, a typed generic consumer with dead-letter queue support, and a zero-boilerplate producer.

---

## Installation

```bash
npm install rabbitmq-common amqplib
# or
yarn add rabbitmq-common amqplib
```

> This library is distributed as source / peer to `amqplib`. Install `amqplib` alongside it.

---

## Quick Start

### Publishing a message

```typescript
import { Producer } from "./index.js";

const producer = new Producer("amqp://localhost");

await producer.publish("orders", { id: 1, item: "widget", qty: 3 });
await producer.close();
```

### Consuming messages

```typescript
import { Consumer } from "./index.js";
import type { ConsumeMessage } from "amqplib";

interface Order {
  id: number;
  item: string;
  qty: number;
}

class OrderConsumer extends Consumer<Order> {
  async onMessage(data: Order, originalMsg: ConsumeMessage): Promise<void> {
    console.log("Received order:", data);
    // message is auto-acked after this resolves
  }
}

const consumer = new OrderConsumer("amqp://localhost");
await consumer.consume("orders");
```

---

## API Reference

### `ConnectionManager`

A static singleton that manages a single shared AMQP connection per process. Concurrent calls to `getConnection` during startup are safely deduplicated — only one underlying TCP connection is established.

```typescript
const conn = await ConnectionManager.getConnection("amqp://localhost");
```

The connection is automatically cleared on `error` or `close` events, so the next call to `getConnection` will re-establish it.

---

### `BaseRabbit`

Abstract base class shared by both `Producer` and `Consumer`. Handles lazy channel creation and basic channel error recovery.

```typescript
abstract class BaseRabbit {
  protected async getChannel(): Promise<Channel>;
  async close(): Promise<void>;
}
```

The channel is re-created on demand if it drops due to an error.

---

### `Producer`

Publishes JSON-serialised messages to a named queue. Queues are asserted once per queue name per process (tracked via a static `Set`) to avoid redundant wire round-trips.

```typescript
class Producer extends BaseRabbit {
  async publish<T>(queue: string, message: T): Promise<boolean>;
}
```

| Parameter | Type     | Description                                          |
| --------- | -------- | ---------------------------------------------------- |
| `queue`   | `string` | Queue name. Created as durable if it does not exist. |
| `message` | `T`      | Any JSON-serialisable value.                         |

Returns `true` if the message was written to the socket buffer, `false` if the channel's write buffer is full (back-pressure).

**Example**

```typescript
const producer = new Producer("amqp://localhost");
const ok = await producer.publish("emails", {
  to: "user@example.com",
  subject: "Hello",
});
```

---

### `Consumer<T>`

Generic abstract class for building typed consumers. Extend it and implement `onMessage`.

```typescript
abstract class Consumer<T> extends BaseRabbit {
  abstract onMessage(data: T, originalMsg: ConsumeMessage): Promise<void>;
  async onError(
    error: Error,
    data?: T,
    originalMsg?: ConsumeMessage,
  ): Promise<void>;
  async consume(queue: string, options?: ConsumeOptions): Promise<void>;
}
```

#### `consume(queue, options?)`

| Option     | Type      | Default | Description                                                   |
| ---------- | --------- | ------- | ------------------------------------------------------------- |
| `prefetch` | `number`  | `1`     | Max unacknowledged messages in-flight per consumer.           |
| `useDLQ`   | `boolean` | `false` | Automatically set up a dead-letter exchange and failed queue. |

#### Dead-Letter Queue topology

When `useDLQ: true` is set, the library creates:

| Resource              | Name pattern                   |
| --------------------- | ------------------------------ |
| Dead-letter exchange  | `<queue>_dlx` (type: `direct`) |
| Failed messages queue | `<queue>_failed`               |
| Routing key           | `dead-letter`                  |

Messages that throw during `onMessage` are `nack`-ed without requeue and routed to `<queue>_failed` automatically.

#### `onError(error, data?, originalMsg?)`

Called when `onMessage` throws. Override to add custom error reporting, alerting, or structured logging. The default implementation logs to `console.error`.

```typescript
async onError(error: Error, data?: Order): Promise<void> {
  await alertingService.report(error, { payload: data });
}
```

**Full example with DLQ**

```typescript
class PaymentConsumer extends Consumer<Payment> {
  async onMessage(data: Payment, msg: ConsumeMessage): Promise<void> {
    await processPayment(data);
  }

  async onError(error: Error, data?: Payment): Promise<void> {
    console.error("Payment failed:", error.message, data);
  }
}

const consumer = new PaymentConsumer("amqp://localhost");
await consumer.consume("payments", { prefetch: 5, useDLQ: true });
```

---

## Architecture

```
ConnectionManager          (shared singleton connection)
       │
   BaseRabbit              (shared channel lifecycle)
   ┌───┴───┐
Producer  Consumer<T>      (publish / subscribe)
```

- One AMQP connection is shared across all `Producer` and `Consumer` instances in the same process.
- Each instance holds its own channel, allowing independent prefetch and flow control.
- Queue assertion for producers is cached statically — if you need to assert different queue options at runtime, call `channel.assertQueue` directly via `getChannel()`.

---

## Error Handling

| Event                                     | Behaviour                                                               |
| ----------------------------------------- | ----------------------------------------------------------------------- |
| Connection `error` / `close`              | Connection reference is cleared; next operation re-connects.            |
| Channel `error`                           | Channel reference is cleared; next operation recreates the channel.     |
| `onMessage` throws (with `useDLQ: false`) | Message is `nack`-ed and **requeued**.                                  |
| `onMessage` throws (with `useDLQ: true`)  | Message is `nack`-ed, **not requeued**, and routed to `<queue>_failed`. |

---

## TypeScript

All public types from `amqplib` that you need for extending the library are re-exported from the main entry point:

```typescript
import type { ChannelModel, Channel, ConsumeMessage } from "./index.js";
```

---

## Caveats

- `Producer.assertedQueues` is a **static** set shared across all `Producer` instances in the process. If two producers target the same queue name with different options, only the first assertion takes effect.
- This library does not implement automatic reconnection with retry/backoff. On connection loss, the next call that triggers `getChannel()` will attempt to reconnect immediately. Consider wrapping operations in a retry loop for production use.
- Exchange-based routing (fanout, topic, headers) is not abstracted; use `getChannel()` directly for those patterns.
