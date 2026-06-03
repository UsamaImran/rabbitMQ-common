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
    Object.setPrototypeOf(this, new.target.prototype);
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
    Object.setPrototypeOf(this, new.target.prototype);
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
    Object.setPrototypeOf(this, new.target.prototype);
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
