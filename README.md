# rabbitmq-common

[![npm version](https://img.shields.io/npm/v/rabbitmq-common)](https://www.npmjs.com/package/rabbitmq-common)
[![CI](https://github.com/UsamaImran/rabbitMQ-common/actions/workflows/ci.yml/badge.svg)](https://github.com/UsamaImran/rabbitMQ-common/actions/workflows/ci.yml)

A TypeScript-first RabbitMQ utility for Node.js, built on [`amqplib`](https://github.com/amqp-node/amqplib), for producers and consumers that need predictable topology management, dead-letter queues, backpressure handling, and connection/channel recovery.

## Why rabbitmq-common?

`rabbitmq-common` provides a small abstraction around the repetitive parts of RabbitMQ integration without hiding important AMQP behavior.

- Producer and consumer APIs with TypeScript types
- Automatic queue and exchange declaration
- Optional dead-letter exchange (DLX) and dead-letter queue (DLQ) topology
- Channel-scoped topology caching
- Safe batch publishing with backpressure handling
- Consumer recovery after channel/connection loss
- Recovery backoff with optional jitter
- Runtime exchange bindings that are restored after recovery
- Typed errors for connection, publishing, and consumption failures

## Installation

```bash
npm install rabbitmq-common
```

## Quick start

### Producer

```ts
import { Producer } from "rabbitmq-common";

const producer = new Producer("amqp://localhost", {
  useDLQ: true,
});

await producer.publish("orders", {
  id: 1,
  item: "book",
});
```

### Consumer

```ts
import { Consumer, type ConsumeMessage } from "rabbitmq-common";

class OrderConsumer extends Consumer<{ id: number; item: string }> {
  async onMessage(
    data: { id: number; item: string },
    _msg: ConsumeMessage,
  ): Promise<void> {
    console.log("Received order:", data);
  }
}

const consumer = new OrderConsumer("amqp://localhost", {
  useDLQ: true,
});

await consumer.consume("orders");
```

## Dead-letter queues

DLQ behavior is **instance-level topology configuration**. Configure `useDLQ` when creating the `Producer` or `Consumer`; it is not a `consume()` option.

```ts
const consumer = new OrderConsumer("amqp://localhost", {
  useDLQ: true,
});

await consumer.consume("orders");
```

When DLQ support is enabled for `orders`, the library creates this topology:

```text
orders
  │
  │ dead-letter
  ▼
orders_dlx ── dead-letter ──▶ orders_failed
```

The main queue is declared with:

```text
x-dead-letter-exchange = orders_dlx
x-dead-letter-routing-key = dead-letter
```

The DLX is a durable `direct` exchange, and `orders_failed` is bound to it with the `dead-letter` routing key.

### Important RabbitMQ topology rule

RabbitMQ queue and exchange properties are immutable after creation. If `orders` already exists without the requested DLX arguments, enabling DLQ later does **not** modify the existing queue. RabbitMQ rejects the incompatible redeclaration with `406 PRECONDITION_FAILED`.

This is why DLQ configuration belongs to the producer/consumer initialization rather than being supplied to `consume()` at runtime.

If an existing queue needs a different topology, migrate/recreate it before changing the configuration used by the library.

## Publishing

### Single message

```ts
await producer.publish("orders", {
  id: 1,
  item: "book",
});
```

Queue options can be supplied at construction time as defaults or overridden for an individual publish:

```ts
const producer = new Producer("amqp://localhost", {
  queueOptions: {
    durable: true,
    messageTtl: 60_000,
  },
});

await producer.publish(
  "orders",
  { id: 1 },
  {},
  { priority: 5 },
);
```

Supported queue options:

| Option | Type | Description |
| --- | --- | --- |
| `durable` | `boolean` | Whether the queue survives broker restart. Defaults to `true`. |
| `maxLength` | `number` | Maximum number of messages retained by the queue. |
| `messageTtl` | `number` | Message TTL in milliseconds. |
| `priority` | `number` | Maximum supported message priority. |

As with DLQ configuration, changing queue arguments for an already-created queue can result in a RabbitMQ topology error.

### Batch publishing

```ts
const result = await producer.publishBatch("orders", [
  { id: 1 },
  { id: 2 },
  { id: 3 },
]);

console.log(result);
// { total: 3, successful: 3, failed: 0, errors: [] }
```

Batch publishing respects AMQP channel backpressure. When `sendToQueue()` or `publish()` returns `false`, the message has already been accepted into the channel's write buffer. The library waits for `drain` before continuing and does **not** publish that message again.

### Publishing to an exchange

```ts
await producer.publishToExchange(
  "events",
  "topic",
  { event: "order.created", orderId: 1 },
  { routingKey: "orders.created" },
);
```

Supported exchange types are:

- `direct`
- `topic`
- `fanout`

Batch exchange publishing is also available through `publishBatchToExchange()`.

## Consuming

By default, consumers use a prefetch of `1`:

```ts
await consumer.consume("orders");
```

Set a different prefetch value when needed:

```ts
await consumer.consume("orders", {
  prefetch: 10,
});
```

### Consuming from an exchange binding

```ts
await consumer.consume("orders", {
  exchange: "events",
  exchangeType: "topic",
  routingKey: "orders.#",
});
```

Additional bindings can be managed while consuming:

```ts
await consumer.bindQueue(
  "orders",
  "events",
  "topic",
  "payments.#",
);

await consumer.unbindQueue(
  "orders",
  "events",
  "payments.#",
);
```

Bindings created through `bindQueue()` are tracked as desired runtime topology and restored automatically after consumer recovery.

## Error handling

Consumer message processing follows these rules:

| Situation | Behavior |
| --- | --- |
| `onMessage()` succeeds | Message is acknowledged. |
| `onMessage()` fails with DLQ enabled | Message is negatively acknowledged without requeue and can be routed to the DLQ. |
| `onMessage()` fails with DLQ disabled | Message is negatively acknowledged and requeued. |
| Malformed JSON | Message is not requeued. |
| Topology declaration fails | Error is surfaced rather than endlessly retrying an invalid declaration. |
| Channel/connection is lost | Consumer recovery can recreate the channel and consumer. |

The library exposes typed errors:

```ts
import {
  RabbitConnectionError,
  RabbitPublishError,
  RabbitConsumeError,
} from "rabbitmq-common";
```

## Consumer recovery

Consumer recovery is designed to avoid competing recovery loops when RabbitMQ emits multiple failure events.

The recovery process:

1. Detects channel/connection loss.
2. Ensures only one recovery loop is active.
3. Waits using exponential backoff with optional jitter.
4. Creates a replacement channel.
5. Reasserts queue topology.
6. Restores tracked exchange bindings.
7. Recreates the consumer.

Example recovery configuration:

```ts
const consumer = new OrderConsumer("amqp://localhost", {
  maxRecoverRetries: 10,
  backoffBase: 1_000,
  maxBackoff: 30_000,
  recoveryJitter: 0.2,
});
```

Set `maxRecoverRetries` to `-1` for unlimited recovery attempts. Recovery stops for permanent topology failures such as `403`, `404`, `405`, `406`, or equivalent RabbitMQ topology/access errors.

You can also trigger recovery manually:

```ts
await consumer.forceRecover();
```

## Connection and lifecycle

The library shares RabbitMQ connections by URL through `ConnectionManager`, while producers and consumers maintain their own channels.

```ts
console.log(producer.isConnected());
console.log(producer.isChannelReady());

await producer.close();
await consumer.close();
```

`ConnectionManager` also exposes connection-level lifecycle management:

```ts
await ConnectionManager.close();
```

## API overview

### `Producer`

| Method | Purpose |
| --- | --- |
| `publish(queue, message, publishOptions?, queueOptions?)` | Publish a message directly to a queue. |
| `publishBatch(queue, messages, publishOptions?, queueOptions?)` | Publish multiple messages with backpressure handling. |
| `publishToExchange(exchange, type, message, options?)` | Publish a message to an exchange. |
| `publishBatchToExchange(exchange, type, messages, options?)` | Batch publish to an exchange. |
| `waitForDrain()` | Wait for channel backpressure to clear. |
| `resetQueueCache(queue?)` | Reset queue assertion cache. |
| `resetExchangeCache(exchange?, type?)` | Reset exchange assertion cache. |
| `close()` | Close the producer's channel. |
| `isConnected()` | Check whether the shared connection is available. |
| `isChannelReady()` | Check whether the producer has an active channel. |

### `Consumer<T>`

| Method | Purpose |
| --- | --- |
| `consume(queue, options?)` | Start consuming from a queue. |
| `bindQueue(queue, exchange, exchangeType, routingKey?)` | Add and track a runtime binding. |
| `unbindQueue(queue, exchange, routingKey?)` | Remove a runtime binding. |
| `forceRecover()` | Trigger consumer recovery manually. |
| `isActive()` | Check whether consumption is active. |
| `getCurrentQueue()` | Get the current queue. |
| `getActiveBindings()` | Get tracked runtime bindings. |
| `close()` | Stop the consumer and close its channel. |

### Constructor options

Common options:

```ts
interface BaseRabbitOptions {
  maxRetries?: number;
  logger?: Logger;
  useDLQ?: boolean;
  queueOptions?: QueueOptions;
}
```

Consumer-specific options:

```ts
interface ConsumerOptions extends BaseRabbitOptions {
  maxRecoverRetries?: number;
  backoffBase?: number;
  maxBackoff?: number;
  recoveryJitter?: number;
}
```

## Design notes

### Channel-scoped topology caching

Queue and exchange assertion caches are tied to the AMQP channel. When a channel closes or errors, the cache is invalidated so topology is asserted again on the replacement channel.

This prevents stale in-memory topology state from being reused after recovery.

### Topology is not migration

The library uses RabbitMQ declarations to ensure the desired topology exists. It does not attempt to mutate existing queues or exchanges when their immutable properties differ.

This distinction is especially important for DLQ configuration: enabling DLQ for a queue that was previously created without the required dead-letter arguments requires an explicit broker-side migration/recreation.

## Documentation

The root README is the **current v6 documentation**. Earlier version documentation is preserved under `docs/`:

- [v1 documentation](./docs/v1.md)
- [v2 documentation](./docs/v2.md)
- [v3 documentation](./docs/v3.md)
- [v4 documentation](./docs/v4.md)
- [v5 documentation](./docs/v5.md)
- [v6 reliability notes](./technical/v6-reliability.md)

The version-specific documents are historical; this README describes the current v6 API and behavior.

## Development

```bash
npm install
npm run build
npm test
npm run test:coverage
```

CI runs the build and test suite on pushes and pull requests targeting `main` using Node.js 20.

## License

MIT
