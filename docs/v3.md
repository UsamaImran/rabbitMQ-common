# rabbitmq-common v3

A lightweight, type-safe RabbitMQ client for Node.js built on top of [amqplib](https://www.npmjs.com/package/amqplib).

`rabbitmq-common v3` is a correctness and reliability release built on top of v2. After v2 shipped, real-world usage at scale exposed a set of bugs that required applications to work around library limitations. v3 fixes these at the source, adds a pluggable logger interface, typed errors, and exposes configuration that was previously hardcoded.

While v2 solved the recovery and DLQ problems introduced in production, further issues emerged:

- multiple broker URLs silently shared one connection
- consumer listeners stacked on each recovery cycle, multiplying callbacks
- malformed messages caused infinite requeue loops
- recovery retried forever with no backoff and no way to limit attempts
- `ConnectionManager` could not reconnect after `close()` was called
- `onError` was not awaited, swallowing errors thrown inside the hook
- `console` was hardcoded — no way to plug in an application logger
- there were no typed errors for catch blocks to target

v3 solves these problems by introducing:

- per-URL connection isolation
- listener cleanup on recovery
- malformed message protection
- exponential backoff and retry limits on consumer recovery
- reusable `ConnectionManager` after shutdown
- awaited `onError` with secondary error capture
- pluggable `Logger` interface
- typed error classes (`RabbitConnectionError`, `RabbitPublishError`, `RabbitConsumeError`)
- `PublishOptions` and `QueueOptions` on `Producer`
- `close()` and `isConnected()` on `Producer` and `Consumer`

> ⚠️ v3 contains breaking changes.
> Please read the migration guide before upgrading from v2.

Looking for older documentation?

- [v2 README](./docs/v2.md)
- [v1 README](./docs/v1.md)

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
- Pluggable logger interface — drop in Winston, Pino, or any compatible logger
- Typed error classes for precise catch blocks
- `close()` and `isConnected()` on all classes
- Consumer lifecycle error hooks
- Fully typed with TypeScript
- Minimal boilerplate

---

# Why v3?

v2 worked well for most RabbitMQ workflows, but production deployments at scale exposed issues the library could not protect against:

- services connecting to multiple brokers received connections for the wrong URL
- consumers that recovered once would fire duplicate recovery callbacks on the next drop
- a single bad message with malformed JSON could spin a queue indefinitely
- there was no way to limit recovery attempts or add backoff between them
- after a graceful shutdown, the connection manager could not restart without a process restart
- errors thrown inside `onError` were silently discarded
- `console` output could not be redirected to the application logger

v3 moves these concerns into the library so applications remain simpler, safer, and easier to operate.

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

# What's New in v3

## Per-URL Connection Isolation

v2 stored a single global connection regardless of URL. If two consumers connected to different brokers, the second silently received the wrong connection.

v3 keys connections by URL — each broker gets its own isolated connection.

```ts
// v3 — these connect to two separate brokers correctly
const producerA = new Producer("amqp://broker-a");
const producerB = new Producer("amqp://broker-b");
```

---

## Consumer Listener Stacking Fixed

v2 registered a new `channel.on("close")` and `channel.on("error")` listener on every recovery cycle. After one recovery, two callbacks fired on the next drop; after two, four — growing unboundedly.

v3 calls `channel.removeAllListeners()` before re-registering listeners, keeping exactly one active at all times.

---

## Malformed Message Protection

v2 nack'd all failures with `requeue: true` on non-DLQ queues, including JSON parse errors. A message with malformed JSON would requeue and loop indefinitely.

v3 distinguishes parse errors from handler errors. Malformed messages are always discarded or routed to the DLQ — never requeued.

| Scenario           | `useDLQ: false` | `useDLQ: true` |
| ------------------ | --------------- | -------------- |
| `onMessage` throws | Requeued        | Routed to DLQ  |
| JSON parse error   | Discarded       | Routed to DLQ  |

---

## Recovery Backoff and Retry Limit

v2 retried consumer recovery every 5 seconds forever with no backoff and no way to stop it.

v3 uses the same exponential backoff as the connection layer and exposes `maxRecoverRetries` to limit attempts.

```txt
1s → 2s → 4s → 8s → 16s → 30s max
```

```ts
const consumer = new OrderConsumer("amqp://localhost", {
  maxRecoverRetries: 10,
});
```

---

## `onError` Is Now Awaited

v2 called `onError` without awaiting the result. Errors thrown inside the hook were silently swallowed.

v3 awaits `onError` and separately catches and logs any error it throws, so nothing is lost silently.

---

## `ConnectionManager` Reusable After `close()`

v2 permanently set `isShuttingDown = true` on `close()`. Any reconnect attempt after a graceful shutdown — common in tests and staged restarts — would always throw.

v3 resets the flag after closing so the manager can reconnect cleanly without a process restart.

---

## Pluggable Logger

v2 called `console.warn` and `console.error` directly with no interception point.

v3 accepts any `Logger`-compatible object (`{ info, warn, error }`) on every class. Defaults to `console` so existing code requires no changes.

```ts
import pino from "pino";

const logger = pino();
const producer = new Producer("amqp://localhost", { logger });
const consumer = new OrderConsumer("amqp://localhost", { logger });
ConnectionManager.setLogger(logger);
```

---

## Typed Errors

v3 exports typed error classes so catch blocks can target specific failure types without string-matching.

```ts
import { RabbitPublishError } from "rabbitmq-common";

try {
  await producer.publish("orders", payload);
} catch (err) {
  if (err instanceof RabbitPublishError) {
    console.error(`Failed on queue "${err.queue}":`, err.message);
  }
}
```

---

## Per-Instance Queue Assertion Cache

v2 cached asserted queues in a static `Set` shared across all `Producer` instances. Two producers with different configurations for the same queue name would silently skip the second assertion.

v3 makes the cache per-instance so each `Producer` manages its own queue state independently.

---

# API

## `Producer`

### Constructor

```ts
new Producer(url: string, options?: BaseRabbitOptions)
```

| Option       | Type     | Default   | Description                                   |
| ------------ | -------- | --------- | --------------------------------------------- |
| `maxRetries` | `number` | `5`       | Connection retry attempts. `-1` for infinite. |
| `logger`     | `Logger` | `console` | Custom logger instance                        |

---

### `publish<T>(queue, message, publishOptions?, queueOptions?)`

Publishes a persistent message to a durable queue. The queue is asserted on first use and cached — subsequent publishes skip the assertion round-trip.

```ts
await producer.publish("orders", { id: 1, item: "book" });
```

With options:

```ts
await producer.publish(
  "orders",
  { id: 1, item: "book" },
  { persistent: true, expiration: "60000" },
  { durable: true, maxLength: 1000 },
);
```

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

Returns `Promise<boolean>`. `false` means the socket write buffer is full — wait for the channel's `drain` event before sending more.

---

### `resetQueueCache(queue?)`

Clears the internal queue assertion cache. Pass a queue name to reset one entry, or call with no arguments to reset all. Useful after reconnects when queue configuration may have changed.

```ts
producer.resetQueueCache("orders"); // reset one
producer.resetQueueCache(); // reset all
```

---

### `close()`

Closes the producer's channel. Safe to call multiple times.

```ts
await producer.close();
```

---

### `isConnected()`

Returns `true` if the underlying broker connection is currently active.

```ts
if (!producer.isConnected()) {
  console.warn("Broker is down");
}
```

---

## `Consumer<T>`

Abstract class for consuming typed RabbitMQ messages. Extend it and implement `onMessage`.

### Constructor

```ts
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

| Parameter     | Type                          | Description                          |
| ------------- | ----------------------------- | ------------------------------------ |
| `error`       | `Error`                       | The error thrown by `onMessage`      |
| `data`        | `T \| undefined`              | Parsed payload, if parsing succeeded |
| `originalMsg` | `ConsumeMessage \| undefined` | Raw amqplib message                  |

Useful for:

- structured logging
- Sentry / error tracking integration
- failed payload inspection
- metrics and alerting

### Example

```ts
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

```ts
await consumer.consume("payments", {
  useDLQ: true,
});
```

**Options**

| Option     | Type      | Default | Description                     |
| ---------- | --------- | ------- | ------------------------------- |
| `prefetch` | `number`  | `1`     | Maximum unacknowledged messages |
| `useDLQ`   | `boolean` | `false` | Enables automatic DLQ setup     |

---

### Dead-Letter Queue Support

Enable DLQ support directly from the consumer:

```ts
await consumer.consume("payments", {
  useDLQ: true,
});
```

When enabled, the following resources are created automatically:

| Resource | Naming Convention |
| -------- | ----------------- |
| DLX      | `${queue}_dlx`    |
| DLQ      | `${queue}_failed` |

Failed messages are routed to `${queue}_failed` instead of being requeued. Malformed messages are always dead-lettered — never requeued — regardless of this setting.

---

### `close()`

Closes the consumer's channel. Safe to call multiple times.

```ts
await consumer.close();
```

---

### `isConnected()`

Returns `true` if the underlying broker connection is currently active.

---

## `ConnectionManager`

Centralized connection management. `Producer` and `Consumer` use this internally — you only need it directly for shutdown or health checks.

```ts
import { ConnectionManager } from "rabbitmq-common";
```

### `ConnectionManager.close(url?)`

Closes connections and resets state. Pass a URL to close one connection, or call with no arguments to close all.

```ts
await ConnectionManager.close(); // close all
await ConnectionManager.close("amqp://localhost"); // close one
```

### `ConnectionManager.isConnected(url)`

Returns `true` if a connection for the given URL is currently active.

```ts
ConnectionManager.isConnected("amqp://localhost"); // boolean
```

### `ConnectionManager.setLogger(logger)`

Sets a global logger for all connection-level output.

```ts
ConnectionManager.setLogger(pino());
```

---

# Error Handling

Errors thrown inside `onMessage` are automatically handled. Behavior depends on DLQ configuration:

| DLQ Enabled | Behavior                 |
| ----------- | ------------------------ |
| `false`     | Message is requeued      |
| `true`      | Message is routed to DLQ |

JSON parse errors are always discarded or routed to the DLQ — never requeued.

### Example

```ts
class MyConsumer extends Consumer<Job> {
  async onMessage(data: Job) {
    if (!data.id) {
      throw new Error("Invalid payload");
    }
  }
}
```

### Typed Errors

```ts
import {
  RabbitPublishError,
  RabbitConnectionError,
  RabbitConsumeError,
} from "rabbitmq-common";
```

| Error                   | Thrown when                        | Extra properties |
| ----------------------- | ---------------------------------- | ---------------- |
| `RabbitConnectionError` | Connection fails after all retries | `cause`          |
| `RabbitPublishError`    | `publish()` fails                  | `queue`, `cause` |
| `RabbitConsumeError`    | Consume setup fails                | `queue`, `cause` |

---

# Logger Interface

Any object implementing `{ info, warn, error }` is a valid logger:

```ts
import winston from "winston";

const logger = winston.createLogger({ ... });

const producer = new Producer("amqp://localhost", { logger });
const consumer = new OrderConsumer("amqp://localhost", { logger });
ConnectionManager.setLogger(logger);
```

Defaults to `console` — existing code requires no changes.

---

# Graceful Shutdown

```ts
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

```ts
app.get("/health", (_req, res) => {
  const ok = producer.isConnected();
  res.status(ok ? 200 : 503).json({ rabbit: ok ? "up" : "down" });
});
```

---

# Exports

```ts
import {
  Producer,
  Consumer,
  ConnectionManager,
  BaseRabbit,

  // Errors
  RabbitConnectionError,
  RabbitPublishError,
  RabbitConsumeError,
} from "rabbitmq-common";
```

## Re-exported Types

```ts
import type {
  Logger,
  ConsumeOptions,
  PublishOptions,
  QueueOptions,
  BaseRabbitOptions,

  // amqplib re-exports
  Channel,
  ChannelModel,
  ConsumeMessage,
} from "rabbitmq-common";
```

---

# Migration Guide

## From v2

### Constructor — `BaseRabbitOptions`

`Producer` and `Consumer` now accept an options object as the second argument. The argument is optional — existing code works without changes.

```ts
// v2 — still works
const producer = new Producer("amqp://localhost");

// v3 — opt into new configuration
const producer = new Producer("amqp://localhost", {
  maxRetries: 10,
  logger: myLogger,
});
```

---

### `publish()` — New Options Parameters

`publish()` now accepts optional `PublishOptions` and `QueueOptions` as third and fourth arguments. Existing calls without them continue to work with the same defaults as v2.

```ts
// v2 — still works
await producer.publish("orders", payload);

// v3 — opt into message TTL, priority, or queue limits
await producer.publish("orders", payload, { expiration: "60000" });
```

---

### `ConnectionManager.close()`

Now accepts an optional URL. Calls without arguments behave identically to v2.

```ts
// v2 — still works
await ConnectionManager.close();

// v3 — new: close one specific connection
await ConnectionManager.close("amqp://localhost");
```

---

### Queue Assertion Cache Is Now Per-Instance

v2 cached queue assertions in a static `Set` shared across all `Producer` instances and documented this as a feature.

In practice this was a bug: two producers with different configurations for the same queue name would silently skip the second assertion. v3 corrects this by making the cache per-instance.

This is a **behavioral breaking change** for applications that intentionally created multiple `Producer` instances expecting shared deduplication. For the vast majority of applications — one producer per service — no change is required.

---

## From v1

Follow the [v2 migration guide](./docs/v2.md#migration-guide) first, then apply the v2 → v3 changes above.

---

# Future Directions

The following capabilities are planned for upcoming releases. If any of these are relevant to your use case, feel free to open an issue or upvote an existing one.

---

## Exchange Support

v1 through v3 are queue-only. All publishing goes directly to a named queue via the default exchange.

A future release will introduce first-class exchange support, covering the three patterns RabbitMQ applications commonly need:

**Fanout** — broadcast a message to all bound queues. Useful for cache invalidation, event broadcasting, and notifying multiple services of the same event.

```ts
// planned API
await producer.publishToExchange("notifications", "fanout", payload);
```

**Topic** — route messages to queues based on a routing key pattern. Useful for multi-tenant systems, environment-scoped events, and selective subscriptions.

```ts
// planned API
await producer.publishToExchange("events", "topic", payload, {
  routingKey: "orders.created.eu",
});
```

**Direct** — route messages to a specific queue by exact routing key. Useful for task routing, priority lanes, and explicit service-to-service addressing.

```ts
// planned API
await producer.publishToExchange("tasks", "direct", payload, {
  routingKey: "email",
});
```

Consumers will gain a corresponding `bindQueue` option to declare and bind queues to exchanges at consume time, keeping the zero-boilerplate model the library is built around.

---

## RPC (Request / Reply)

Support for the RabbitMQ RPC pattern — send a message and await a correlated reply — is planned as a first-class abstraction.

```ts
// planned API
const rpc = new RpcClient("amqp://localhost");

const result = await rpc.call("user-service", { userId: 42 });
```

This will handle correlation ID generation, reply queue lifecycle, and timeout management internally. A corresponding `RpcServer` abstraction will make it straightforward to implement the handler side.

---

## Publish Confirms

Currently, `publish()` returns a boolean reflecting the socket write buffer state but does not wait for broker acknowledgement. A message can be accepted by the socket layer and still be lost if the broker crashes before writing it to disk.

A future release will add opt-in confirm channel support:

```ts
// planned API
const producer = new Producer("amqp://localhost", {
  confirms: true,
});

await producer.publish("orders", payload); // resolves only after broker ack
```

---

## Batch Publishing

Publishing large volumes of messages requires multiple `await producer.publish()` calls today. A future release will add a `publishBatch()` method that sends multiple messages in a single channel operation and resolves once all are flushed.

```ts
// planned API
await producer.publishBatch("orders", [
  { id: 1, item: "book" },
  { id: 2, item: "pen" },
]);
```

---

## Connection and Recovery Event Hooks

There is currently no way to observe connection lifecycle events from outside the library. A future release will expose hooks for connection and recovery events, making it easier to integrate with monitoring systems, update health state, or trigger application-level logic on reconnect.

```ts
// planned API
const producer = new Producer("amqp://localhost", {
  onConnected: () => metrics.gauge("rabbit.connected", 1),
  onDisconnected: () => metrics.gauge("rabbit.connected", 0),
});
```

---

# Requirements

- Node.js 18+
- RabbitMQ server
- `amqplib` peer dependency

---

# License

MIT
