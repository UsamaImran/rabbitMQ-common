// ─── Logger Interface ─────────────────────────────────────────────────────────
// Allows users to plug in their own logger (Winston, Pino, etc.)
// Defaults to console in all classes.

export interface Logger {
  info(message: string, ...args: unknown[]): void;
  warn(message: string, ...args: unknown[]): void;
  error(message: string, ...args: unknown[]): void;
}

// ─── Custom Error Classes ─────────────────────────────────────────────────────

export class RabbitConnectionError extends Error {
  constructor(
    message: string,
    public readonly cause?: unknown,
  ) {
    super(message);
    this.name = "RabbitConnectionError";
  }
}

export class RabbitPublishError extends Error {
  constructor(
    message: string,
    public readonly queue: string,
    public readonly cause?: unknown,
  ) {
    super(message);
    this.name = "RabbitPublishError";
  }
}

export class RabbitConsumeError extends Error {
  constructor(
    message: string,
    public readonly queue: string,
    public readonly cause?: unknown,
  ) {
    super(message);
    this.name = "RabbitConsumeError";
  }
}

// ─── Shared Option Types ──────────────────────────────────────────────────────

export interface ConsumeOptions {
  prefetch?: number;
  useDLQ?: boolean;
}

export interface PublishOptions {
  persistent?: boolean;
  expiration?: string; // message TTL in ms as string, e.g. "60000"
  priority?: number;
}

export interface QueueOptions {
  durable?: boolean;
  maxLength?: number;
  messageTtl?: number;
  priority?: number;
}
