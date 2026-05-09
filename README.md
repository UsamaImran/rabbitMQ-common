# @usama-imran/rabbitmq-common

A lightweight, type-safe wrapper for RabbitMQ (amqplib) designed for microservices. Built with TypeScript and ESM support.

## Features

- **Type-Safe**: Uses Generics `<T>` for both Producers and Consumers.
- **Abstract Base Classes**: Ensures consistent connection handling across services.
- **Auto-Acknowledgment**: Handles message `ack` and `nack` (with error handling) automatically.
- **ESM Ready**: Fully compatible with modern Node.js environments.

## Installation

```bash
npm install @usama-imran/rabbitmq-common
```
