import { ConsumeMessage } from "amqplib";

// ─── Logger Interface ─────────────────────────────────────────────────────────

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

// ─── Shared Types ─────────────────────────────────────────────────────────────

export interface BaseRabbitOptions {
  /** Maximum retry attempts for connection recovery. -1 for infinite */
  maxRetries?: number;
  /** Logger instance */
  logger?: Logger;
}

// ─── Exchange Types ──────────────────────────────────────────────────────────

export type ExchangeType = "fanout" | "topic" | "direct";

export interface ExchangePublishOptions {
  routingKey?: string;
  persistent?: boolean;
  expiration?: string;
  priority?: number;
}

export interface ExchangeBindOptions {
  exchange: string;
  routingKey?: string;
}

// ─── Producer Types ──────────────────────────────────────────────────────────

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

// ─── Consumer Types ──────────────────────────────────────────────────────────

export interface ConsumeOptions {
  prefetch?: number;
  useDLQ?: boolean;
}

export interface ExchangeConsumeOptions extends ConsumeOptions {
  exchange?: string;
  exchangeType?: ExchangeType;
  routingKey?: string;
}

export interface QueueSetupOptions {
  useDLQ?: boolean;
  queueOptions?: QueueOptions;
}

export interface Binding {
  queue: string;
  exchange: string;
  routingKey: string;
}

export interface RecoveryOptions {
  maxRecoverRetries?: number;
  backoffBase?: number;
  maxBackoff?: number;
}

// ─── Batch Publishing Types ──────────────────────────────────────────────────

export interface BatchPublishResult {
  /** Total messages attempted */
  total: number;
  /** Number of successfully published messages */
  successful: number;
  /** Number of failed messages */
  failed: number;
  /** Detailed errors for failed messages */
  errors: BatchPublishError[];
}

export interface BatchPublishError {
  /** Index of the failed message in the original array */
  index: number;
  /** The original message that failed */
  message: any;
  /** The error that occurred */
  error: Error;
}

// ─── Message Handler Types ──────────────────────────────────────────────────

export interface MessageHandlerCallbacks<T> {
  onMessage: (data: T, msg: ConsumeMessage) => Promise<void>;
  onError: (error: Error, data?: T, msg?: ConsumeMessage) => Promise<void>;
  logger?: Logger;
}

// ─── Re-export amqplib Types ────────────────────────────────────────────────

// These are commonly used types from amqplib that consumers/producers need
export type { Channel, ChannelModel, ConsumeMessage } from "amqplib";

export interface ExchangeConsumeOptions extends ConsumeOptions {
  exchange?: string;
  exchangeType?: ExchangeType;
  routingKey?: string;
}
