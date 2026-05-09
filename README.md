# rabbitmq-common

A lightweight and type-safe RabbitMQ wrapper built on top of `amqplib` for Node.js and TypeScript applications.

## Features

- Simple producer/consumer abstraction
- Shared RabbitMQ connection handling
- Type-safe messages using generics
- Durable queues by default
- Persistent messages by default
- Async consumer support
- Graceful shutdown support
- Prefetch support
- Minimal boilerplate

---

# Installation

```bash
npm install rabbitmq-common amqplib
```

or

```bash
yarn add rabbitmq-common amqplib
```

---

# Usage

## Producer

```ts
import { Producer } from "rabbitmq-common";

const producer = new Producer("amqp://guest:guest@localhost:5672");

await producer.publish("emails", {
  to: "john@example.com",
  subject: "Welcome!",
});
```

---

## Consumer

```ts
import { Consumer } from "rabbitmq-common";
import type { ConsumeMessage } from "amqplib";

type EmailMessage = {
  to: string;
  subject: string;
};

class EmailConsumer extends Consumer<EmailMessage> {
  async onMessage(
    data: EmailMessage,
    originalMsg: ConsumeMessage,
  ): Promise<void> {
    console.log("Received:", data);
  }
}

const consumer = new EmailConsumer("amqp://guest:guest@localhost:5672");

await consumer.consume("emails");
```

---

# API

## Producer

### publish()

```ts
publish<T>(
  queue: string,
  message: T,
  options?: {
    durable?: boolean;
    persistent?: boolean;
  }
)
```

### Options

| Option     | Default | Description                           |
| ---------- | ------- | ------------------------------------- |
| durable    | true    | Makes queue survive RabbitMQ restarts |
| persistent | true    | Persists messages to disk             |

---

## Consumer

### consume()

```ts
consume(
  queue: string,
  options?: {
    durable?: boolean;
    prefetch?: number;
  }
)
```

### Options

| Option   | Default   | Description                    |
| -------- | --------- | ------------------------------ |
| durable  | true      | Makes queue durable            |
| prefetch | undefined | Limits unacknowledged messages |

---

# Graceful Shutdown

```ts
process.on("SIGINT", async () => {
  await producer.close();
  process.exit(0);
});

process.on("SIGTERM", async () => {
  await producer.close();
  process.exit(0);
});
```

---

# Best Practices

- Use `prefetch(1)` for worker queues
- Always acknowledge messages
- Keep consumers idempotent
- Use durable queues in production
- Use dead-letter queues for retries
- Gracefully close RabbitMQ connections on shutdown

---

# Example Use Cases

- Background jobs
- Email workers
- Notification systems
- Event-driven systems
- Microservices communication

---

# License

MIT
