# rabbitmq-common v6 reliability changes

v6 makes queue topology configuration immutable at the `Producer`/`Consumer` instance level and hardens channel recovery.

## DLQ configuration

Dead-lettering is configured when the producer or consumer is initialized:

```ts
const consumer = new OrdersConsumer("amqp://localhost", {
  useDLQ: true,
});

await consumer.consume("orders");
```

`consume()` no longer accepts `useDLQ`. Likewise, producer DLQ behavior is configured on the `Producer` constructor:

```ts
const producer = new Producer("amqp://localhost", {
  useDLQ: true,
});
```

The library declares the DLX, DLQ, binding, and main queue with its dead-letter arguments as one topology configuration. This is intentional: RabbitMQ rejects redeclarations of an existing queue when its properties or arguments differ (`406 PRECONDITION_FAILED`). Existing queues created without the requested DLX must be migrated or deleted before enabling DLQ for them.

## Queue topology

Queue assertion caches are now scoped to the AMQP channel and include the relevant queue configuration. After a channel is replaced, topology is asserted again.

## Backpressure

A `false` return from `sendToQueue()` or `publish()` means the write buffer is full. The message is already accepted by the channel buffer, so v6 waits for `drain` and continues with the next message. It does not publish the same message twice.

## Recovery

Consumer recovery is single-flight, uses exponential backoff with jitter, and restores desired runtime bindings on the replacement channel. Permanent topology declaration errors are not treated as a reason to blindly retry the same declaration.

## Migration from v5

Change:

```ts
await consumer.consume("orders", { useDLQ: true });
```

to:

```ts
const consumer = new OrdersConsumer("amqp://localhost", {
  useDLQ: true,
});

await consumer.consume("orders");
```

If the queue already exists without the DLX configuration, recreate/migrate the queue first. RabbitMQ does not permit changing those queue arguments through a later `assertQueue()` call.
