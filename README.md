# rabbitmq-common v4

A lightweight, type-safe RabbitMQ client for Node.js built on top of [amqplib](https://www.npmjs.com/package/amqplib).

`rabbitmq-common v4` is a feature release built on top of v3. After v3 shipped fixes for connection isolation, listener stacking, and malformed message protection, the most frequently requested capability was first-class exchange support. v4 introduces that — covering fanout, topic, and direct exchange patterns — while keeping the same zero-boilerplate model the library is built around.

v4 also ships several smaller improvements discovered during v3 production use:

- `publishToExchange()` on `Producer` for all three exchange patterns
- `ExchangeConsumeOptions` on `Consumer` — bind a queue to an exchange at consume time
- `bindQueue()` and `unbindQueue()` on `Consumer` for runtime binding management
- `ExchangeManager` — per-instance exchange assertion cache, exported for advanced use
- `resetExchangeCache()` on `Producer` — mirrors `resetQueueCache()` for exchanges
- `waitForDrain()` on `Producer` — resolves when the channel's write buffer clears
- `forceRecover()` on `Consumer` — manually trigger recovery without waiting for a channel event
- `getCurrentQueue()` on `Consumer` — inspect which queue is currently active
- `isChannelReady()` and `getUrl()` on all classes — richer health and introspection surface

> ⚠️ v4 contains breaking changes.
> Please read the migration guide before upgrading from v3.

Looking for older documentation?

- [v3 README](https://github.com/UsamaImran/rabbitMQ-common/blob/main/docs/v3.md)
- [v2 README](https://github.com/UsamaImran/rabbitMQ-common/blob/main/docs/v2.md)
- [v1 README](https://github.com/UsamaImran/rabbitMQ-common/blob/main/docs/v1.md)

---

# Features

- Simple `Producer` and `Consumer` abstractions
- Automatic connection and channel management
- Per-URL singleton connections via `ConnectionManager`
- Automatic reconnection with exponential backoff
- Consumer auto-recovery with exponential backoff and configurable retry limit
- Built-in Dead Letter Queue (DLQ) support
- Malformed message protection — JSON parse errors never requeue
- Per-instance queue assertion caching
- **Exchange support — fanout, topic, and direct**
- **Per-instance exchange assertion caching**
- **Runtime queue binding and unbinding on `Consumer`**
- Pluggable logger interface — drop in Winston, Pino, or any compatible logger
- Typed error classes for precise catch blocks
- `close()` and `isConnected()` on all classes
- Consumer lifecycle error hooks
- Fully typed with TypeScript
- Minimal boilerplate

---

# Why v4?

v3 worked well for queue-only workflows, but production deployments commonly needed patterns that required dropping down to `amqplib` directly:

- broadcasting the same event to multiple services required one queue per consumer with no shared exchange
- routing messages to different queues based on a key required manual exchange setup outside the library
- there was no way to bind a consumer's queue to an existing exchange at consume time
- exchange assertion was not cached, so high-throughput services re-asserted on every publish

v4 moves these concerns into the library so applications remain simpler and consistent whether they use queues directly or exchanges.

---

# Installation

```
npm install rabbitmq-common
```

---

# Quick Start

## Publishing to a queue

```typescript
import { Producer } from "rabbitmq-common";

const producer = new Producer("amqp://localhost");

await producer.publish("orders", {
  id: 1,
  item: "book",
});
```

## Publishing to an exchange

```typescript
import { Producer } from "rabbitmq-common";

const producer = new Producer("amqp://localhost");

// Fanout — broadcast to all bound queues
await producer.publishToExchange("notifications", "fanout", {
  event: "user.signup",
  userId: 42,
});

// Topic — route by pattern
await producer.publishToExchange(
  "events",
  "topic",
  { userId: 42 },
  {
    routingKey: "orders.created.eu",
  },
);

// Direct — route by exact key
await producer.publishToExchange(
  "tasks",
  "direct",
  { type: "email" },
  {
    routingKey: "email",
  },
);
```

## Consuming from a queue

```typescript
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

## Consuming from an exchange-bound queue

```typescript
class NotificationConsumer extends Consumer<{ event: string; userId: number }> {
  async onMessage(data: { event: string; userId: number }) {
    console.log("Notification:", data);
  }
}

const consumer = new NotificationConsumer("amqp://localhost");

// Assert the queue and bind it to the exchange in one call
await consumer.consume("notifications-service-a", {
  exchange: "notifications",
  exchangeType: "fanout",
});
```

---

# What's New in v4

## Exchange Support

v3 was queue-only — all publishing went directly to a named queue via the default exchange. Any pattern requiring fanout, topic routing, or direct exchange had to be wired manually outside the library.

v4 introduces first-class exchange support covering the three patterns RabbitMQ applications commonly need.

### Fanout

Broadcast a message to every queue bound to the exchange. Useful for cache invalidation, event broadcasting, and notifying multiple services of the same event.

```typescript
await producer.publishToExchange("notifications", "fanout", payload);
```

### Topic

Route messages to queues based on a routing key pattern. Useful for multi-tenant systems, environment-scoped events, and selective subscriptions.

```typescript
await producer.publishToExchange("events", "topic", payload, {
  routingKey: "orders.created.eu",
});
```

Consumers can subscribe to a pattern:

```typescript
await consumer.consume("eu-orders", {
  exchange: "events",
  exchangeType: "topic",
  routingKey: "orders.*.eu",
});
```

### Direct

Route messages to a specific queue by exact routing key. Useful for task routing, priority lanes, and explicit service-to-service addressing.

```typescript
await producer.publishToExchange("tasks", "direct", payload, {
  routingKey: "email",
});
```

---

## Per-Instance Exchange Assertion Cache

Exchange assertion is now cached per `Producer` instance — the same pattern as queue assertion caching introduced in v3. On the first `publishToExchange()` call for a given exchange and type, the exchange is asserted. Subsequent publishes skip the assertion round-trip.

The cache key is `${exchange}:${type}`. Two producers with different configurations for the same exchange name each manage their own assertion independently.

Use `resetExchangeCache()` to invalidate entries after a reconnect or configuration change:

```typescript
producer.resetExchangeCache("notifications", "fanout"); // reset one
producer.resetExchangeCache("notifications"); // reset all types for this exchange
producer.resetExchangeCache(); // reset all
```

---

## Exchange Binding on Consumer

`Consumer.consume()` now accepts `exchange`, `exchangeType`, and `routingKey` in its options. When provided, the queue is asserted, the exchange is asserted, and the binding is created — all in one call.

```typescript
await consumer.consume("payments-audit", {
  exchange: "payments",
  exchangeType: "topic",
  routingKey: "payments.#",
  useDLQ: true,
});
```

DLQ setup and exchange binding compose — enabling both flags wires the dead-letter exchange and binds the queue to the application exchange in the same consume call.

---

## Runtime Binding Management

`Consumer` exposes `bindQueue()` and `unbindQueue()` for adding and removing exchange bindings after consume has started. This is useful for dynamic subscription changes without restarting the consumer.

```typescript
// Add a new routing key subscription at runtime
await consumer.bindQueue("payments-audit", "payments", "payments.refund.#");

// Remove a subscription
await consumer.unbindQueue("payments-audit", "payments", "payments.#");
```

Active bindings are tracked per instance and cleaned up automatically on `close()`.

---

## `waitForDrain()` on Producer

`publish()` and `publishToExchange()` return `false` when the channel's write buffer is full. Previously there was no built-in way to wait for the buffer to clear. v4 adds `waitForDrain()`:

```typescript
const flushed = await producer.publish("orders", payload);

if (!flushed) {
  await producer.waitForDrain();
  // buffer is clear — safe to publish again
}
```

---

## `forceRecover()` on Consumer

Recovery previously triggered only when the channel emitted a `close` or `error` event. v4 exposes `forceRecover()` so application code can trigger recovery directly — useful in test environments or when an external health check detects a stale consumer.

```typescript
await consumer.forceRecover();
```

Recovery uses the same exponential backoff and `maxRecoverRetries` limit as automatic recovery.

---

## Richer Introspection

All classes now expose additional inspection methods:

| Method              | Returns               | Description                                     |
| ------------------- | --------------------- | ----------------------------------------------- |
| `isChannelReady()`  | `boolean`             | `true` if the channel is open and available     |
| `getUrl()`          | `string`              | The broker URL this instance is connected to    |
| `getCurrentQueue()` | `string \| undefined` | The queue name currently active on a `Consumer` |

---

# API

## `Producer`

### Constructor

```typescript
new Producer(url: string, options?: BaseRabbitOptions)
```

| Option       | Type     | Default   | Description                                   |
| ------------ | -------- | --------- | --------------------------------------------- |
| `maxRetries` | `number` | `5`       | Connection retry attempts. `-1` for infinite. |
| `logger`     | `Logger` | `console` | Custom logger instance                        |

---

### `publish<T>(queue, message, publishOptions?, queueOptions?)`

Publishes a persistent message to a durable queue. The queue is asserted on first use and cached — subsequent publishes skip the assertion round-trip.

```typescript
await producer.publish("orders", { id: 1, item: "book" });
```

With options:

```typescript
await producer.publish(
  "orders",
  { id: 1, item: "book" },
  { persistent: true, expiration: "60000" },
  { durable: true, maxLength: 1000 },
);
```

Returns `Promise<boolean>`. `false` means the socket write buffer is full — call `waitForDrain()` before sending more.

**`PublishOptions`**

| Option       | Type      | Default | Description                                            |
| ------------ | --------- | ------- | ------------------------------------------------------ |
| `persistent` | `boolean` | `true`  | Message survives broker restart                        |
| `expiration` | `string`  | —       | Message TTL in milliseconds, e.g. `"60000"`            |
| `priority`   | `number`  | —       | Message priority (requires `maxPriority` on the queue) |

**`QueueOptions`**

| Option       | Type      | Default | Description                            |
| ------------ | --------- | ------- | -------------------------------------- |
| `durable`    | `boolean` | `true`  | Queue survives broker restart          |
| `maxLength`  | `number`  | —       | Max messages before oldest are dropped |
| `messageTtl` | `number`  | —       | Per-queue message TTL in milliseconds  |
| `priority`   | `number`  | —       | Sets `maxPriority` on the queue        |

---

### `publishToExchange<T>(exchange, type, message, options?)`

Publishes a message to an exchange. The exchange is asserted on first use per instance and cached.

```typescript
await producer.publishToExchange("events", "topic", payload, {
  routingKey: "orders.created.eu",
});
```

| Parameter  | Type                     | Description                          |
| ---------- | ------------------------ | ------------------------------------ |
| `exchange` | `string`                 | Exchange name                        |
| `type`     | `ExchangeType`           | `"fanout"`, `"topic"`, or `"direct"` |
| `message`  | `T`                      | Message payload — serialized as JSON |
| `options`  | `ExchangePublishOptions` | Optional publish options             |

**`ExchangePublishOptions`**

| Option       | Type      | Default | Description                                                           |
| ------------ | --------- | ------- | --------------------------------------------------------------------- |
| `routingKey` | `string`  | `""`    | Routing key. Required for `topic` and `direct`; ignored for `fanout`. |
| `persistent` | `boolean` | `true`  | Message survives broker restart                                       |
| `expiration` | `string`  | —       | Message TTL in milliseconds                                           |
| `priority`   | `number`  | —       | Message priority                                                      |

Returns `Promise<boolean>`. `false` means the write buffer is full.

---

### `waitForDrain()`

Resolves when the channel's write buffer drains. Call after `publish()` or `publishToExchange()` returns `false`.

```typescript
await producer.waitForDrain();
```

---

### `resetQueueCache(queue?)`

Clears the queue assertion cache. Pass a queue name to reset one entry, or call with no arguments to reset all.

```typescript
producer.resetQueueCache("orders"); // reset one
producer.resetQueueCache(); // reset all
```

---

### `resetExchangeCache(exchange?, type?)`

Clears the exchange assertion cache. Pass an exchange name and type to reset one entry, an exchange name alone to reset all types for that exchange, or call with no arguments to reset all.

```typescript
producer.resetExchangeCache("events", "topic"); // reset one
producer.resetExchangeCache("events"); // reset all types for this exchange
producer.resetExchangeCache(); // reset all
```

---

### `close()`

Closes the producer's channel. Safe to call multiple times.

```typescript
await producer.close();
```

---

### `isConnected()`

Returns `true` if the underlying broker connection is currently active.

```typescript
if (!producer.isConnected()) {
  console.warn("Broker is down");
}
```

---

### `isChannelReady()`

Returns `true` if the channel is open and ready to use.

---

### `getUrl()`

Returns the broker URL this producer is configured for.

---

## `Consumer<T>`

Abstract class for consuming typed RabbitMQ messages. Extend it and implement `onMessage`.

### Constructor

```typescript
new YourConsumer(url: string, options?: BaseRabbitOptions & { maxRecoverRetries?: number })
```

| Option              | Type     | Default   | Description                                                  |
| ------------------- | -------- | --------- | ------------------------------------------------------------ |
| `maxRetries`        | `number` | `5`       | Connection retry attempts                                    |
| `maxRecoverRetries` | `number` | `-1`      | Max recovery attempts after channel loss. `-1` for infinite. |
| `logger`            | `Logger` | `console` | Custom logger instance                                       |

---

### `onMessage(data, originalMsg)` _(abstract)_

Called for every incoming message. Return normally to ack. Throw to trigger `onError` and nack.

| Parameter     | Type             | Description            |
| ------------- | ---------------- | ---------------------- |
| `data`        | `T`              | Parsed message payload |
| `originalMsg` | `ConsumeMessage` | Raw amqplib message    |

---

### `onError(error, data?, originalMsg?)`

Optional lifecycle hook called when `onMessage` throws. Override to add logging, Sentry reporting, or metrics. Default implementation logs to the configured logger.

```typescript
class EmailConsumer extends Consumer<EmailJob> {
  async onMessage(data: EmailJob) {
    throw new Error("SMTP unavailable");
  }

  async onError(error: Error, data?: EmailJob) {
    await Sentry.captureException(error, { extra: { data } });
  }
}
```

---

### `consume(queue, options?)`

Starts consuming messages from a queue. Automatically recovers on channel or connection loss.

```typescript
await consumer.consume("payments", {
  useDLQ: true,
});
```

With exchange binding:

```typescript
await consumer.consume("payments-audit", {
  exchange: "payments",
  exchangeType: "topic",
  routingKey: "payments.#",
  useDLQ: true,
});
```

**`ExchangeConsumeOptions`**

| Option         | Type           | Default | Description                     |
| -------------- | -------------- | ------- | ------------------------------- |
| `prefetch`     | `number`       | `1`     | Maximum unacknowledged messages |
| `useDLQ`       | `boolean`      | `false` | Enables automatic DLQ setup     |
| `exchange`     | `string`       | —       | Exchange to bind the queue to   |
| `exchangeType` | `ExchangeType` | —       | Required when `exchange` is set |
| `routingKey`   | `string`       | `""`    | Routing key for the binding     |

---

### `bindQueue(queue, exchange, routingKey?)`

Adds a binding between the active queue and an exchange at runtime. The exchange must already exist.

```typescript
await consumer.bindQueue("payments-audit", "payments", "payments.refund.#");
```

Throws if called before `consume()` or with a different queue name than the active one.

---

### `unbindQueue(queue, exchange, routingKey?)`

Removes a binding between the active queue and an exchange.

```typescript
await consumer.unbindQueue("payments-audit", "payments", "payments.#");
```

---

### `forceRecover()`

Manually triggers the recovery process. Uses the same exponential backoff and retry limit as automatic recovery.

```typescript
await consumer.forceRecover();
```

---

### `getCurrentQueue()`

Returns the name of the queue currently being consumed, or `undefined` if `consume()` has not been called.

```typescript
console.log(consumer.getCurrentQueue()); // "payments-audit"
```

---

### Dead-Letter Queue Support

Enable DLQ support directly from the consumer:

```typescript
await consumer.consume("payments", {
  useDLQ: true,
});
```

When enabled, the following resources are created automatically:

| Resource | Naming convention |
| -------- | ----------------- |
| DLX      | `${queue}_dlx`    |
| DLQ      | `${queue}_failed` |

Failed messages are routed to `${queue}_failed` instead of being requeued. Malformed messages are always dead-lettered — never requeued — regardless of this setting. DLQ setup and exchange binding compose correctly when both are specified.

---

### `close()`

Closes the consumer's channel and clears all tracked bindings. Safe to call multiple times.

---

### `isConnected()`

Returns `true` if the underlying broker connection is currently active.

---

### `isChannelReady()`

Returns `true` if the channel is open and ready to use.

---

### `getUrl()`

Returns the broker URL this consumer is configured for.

---

## `ExchangeManager`

Handles exchange assertion with per-instance caching. `Producer` and `Consumer` use this internally — you only need it directly for advanced use cases such as asserting exchanges outside the normal publish/consume flow.

```typescript
import { ExchangeManager } from "rabbitmq-common";
```

### `assertExchange(channel, exchange, type, options?)`

Asserts an exchange. Subsequent calls for the same `exchange:type` pair are no-ops.

```typescript
const manager = new ExchangeManager();
await manager.assertExchange(channel, "events", "topic");
```

| Option    | Type      | Default | Description                      |
| --------- | --------- | ------- | -------------------------------- |
| `durable` | `boolean` | `true`  | Exchange survives broker restart |

### `resetExchangeCache(exchange?, type?)`

Clears the assertion cache using the same semantics as `Producer.resetExchangeCache()`.

---

## `ConnectionManager`

Centralized connection management. `Producer` and `Consumer` use this internally — you only need it directly for shutdown or health checks.

```typescript
import { ConnectionManager } from "rabbitmq-common";
```

### `ConnectionManager.close(url?)`

Closes connections and resets state. Pass a URL to close one connection, or call with no arguments to close all.

```typescript
await ConnectionManager.close(); // close all
await ConnectionManager.close("amqp://localhost"); // close one
```

### `ConnectionManager.isConnected(url)`

Returns `true` if a connection for the given URL is currently active.

```typescript
ConnectionManager.isConnected("amqp://localhost"); // boolean
```

### `ConnectionManager.setLogger(logger)`

Sets a global logger for all connection-level output.

```typescript
ConnectionManager.setLogger(pino());
```

---

# Error Handling

Errors thrown inside `onMessage` are automatically handled. Behavior depends on DLQ configuration:

| DLQ enabled | Behavior                 |
| ----------- | ------------------------ |
| `false`     | Message is requeued      |
| `true`      | Message is routed to DLQ |

JSON parse errors are always discarded or routed to the DLQ — never requeued.

### Typed Errors

```typescript
import {
  RabbitPublishError,
  RabbitConnectionError,
  RabbitConsumeError,
} from "rabbitmq-common";
```

| Error                   | Thrown when                                | Extra properties |
| ----------------------- | ------------------------------------------ | ---------------- |
| `RabbitConnectionError` | Connection fails after all retries         | `cause`          |
| `RabbitPublishError`    | `publish()` or `publishToExchange()` fails | `queue`, `cause` |
| `RabbitConsumeError`    | Consume setup fails                        | `queue`, `cause` |

For `RabbitPublishError` thrown from `publishToExchange()`, the `queue` property holds the exchange name.

---

# Logger Interface

Any object implementing `{ info, warn, error }` is a valid logger:

```typescript
import winston from "winston";

const logger = winston.createLogger({ ... });

const producer = new Producer("amqp://localhost", { logger });
const consumer = new OrderConsumer("amqp://localhost", { logger });
ConnectionManager.setLogger(logger);
```

Defaults to `console` — existing code requires no changes.

---

# Graceful Shutdown

```typescript
import { ConnectionManager } from "rabbitmq-common";

process.on("SIGTERM", async () => {
  await consumer.close();
  await producer.close();
  await ConnectionManager.close();
  process.exit(0);
});
```

---

# Health Checks

```typescript
app.get("/health", (_req, res) => {
  const ok = producer.isConnected() && producer.isChannelReady();
  res.status(ok ? 200 : 503).json({ rabbit: ok ? "up" : "down" });
});
```

---

# Exports

```typescript
import {
  Producer,
  Consumer,
  ConnectionManager,
  BaseRabbit,
  ExchangeManager,

  // Errors
  RabbitConnectionError,
  RabbitPublishError,
  RabbitConsumeError,
} from "rabbitmq-common";
```

## Re-exported Types

```typescript
import type {
  Logger,
  ConsumeOptions,
  PublishOptions,
  QueueOptions,
  ExchangeType,
  ExchangePublishOptions,
  ExchangeConsumeOptions,
  ExchangeBindOptions,
  BaseRabbitOptions,

  // amqplib re-exports
  Channel,
  ChannelModel,
  ConsumeMessage,
} from "rabbitmq-common";
```

---

# Migration Guide

## From v3

### `consume()` — New Exchange Options

`consume()` now accepts optional `exchange`, `exchangeType`, and `routingKey` fields. Existing calls without them continue to work identically.

```typescript
// v3 — still works
await consumer.consume("orders");

// v4 — opt into exchange binding
await consumer.consume("orders", {
  exchange: "events",
  exchangeType: "topic",
  routingKey: "orders.#",
});
```

### `Producer` — New `publishToExchange()` Method

This is additive. Existing `publish()` calls are unchanged.

```typescript
// v3 — still works
await producer.publish("orders", payload);

// v4 — new exchange publishing
await producer.publishToExchange("events", "topic", payload, {
  routingKey: "orders.created",
});
```

### `ExchangeManager` Is Now Exported

`ExchangeManager` was internal in v3 (it did not exist). v4 exports it for advanced use cases. No existing code is affected.

### `ExchangeConsumeOptions` Replaces `ConsumeOptions` on `Consumer`

`ExchangeConsumeOptions` extends `ConsumeOptions` — all existing fields (`prefetch`, `useDLQ`) remain. No changes required for existing consumers.

### New Methods Are Additive

`waitForDrain()`, `forceRecover()`, `getCurrentQueue()`, `isChannelReady()`, and `getUrl()` are all new. No existing code is affected.

---

## From v2

Follow the [v3 migration guide](https://github.com/UsamaImran/rabbitMQ-common/blob/main/docs/v3.md#migration-guide) first, then apply the v3 → v4 changes above.

---

## From v1

Follow the [v2 migration guide](https://github.com/UsamaImran/rabbitMQ-common/blob/main/docs/v2.md#migration-guide) first, then the v2 → v3 guide, then the v3 → v4 changes above.

---

# Requirements

- Node.js 18+
- RabbitMQ server
- `amqplib` peer dependency

---

# License

MIT
