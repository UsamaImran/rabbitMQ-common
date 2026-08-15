import type { Channel, ConsumeMessage } from "amqplib";
import type { Logger } from "../types.js";

export interface MessageHandlerCallbacks<T> {
  onMessage: (data: T, msg: ConsumeMessage) => Promise<void>;
  onError: (error: Error, data?: T, msg?: ConsumeMessage) => Promise<void>;
  logger?: Logger;
}

export class MessageHandler<T> {
  private channel: Channel;
  private callbacks: MessageHandlerCallbacks<T>;
  private useDLQ: boolean;

  constructor(
    channel: Channel,
    callbacks: MessageHandlerCallbacks<T>,
    useDLQ: boolean = false,
  ) {
    this.channel = channel;
    this.callbacks = callbacks;
    this.useDLQ = useDLQ;
  }

  /**
   * Process a single message
   */
  async processMessage(msg: ConsumeMessage): Promise<void> {
    if (!msg) return;

    let content: T | undefined;
    let isParseError = false;

    try {
      // Parse message
      try {
        content = JSON.parse(msg.content.toString()) as T;
      } catch (parseErr: unknown) {
        isParseError = true;
        const message =
          parseErr instanceof Error ? parseErr.message : String(parseErr);
        throw new Error(`Failed to parse message: ${message}`);
      }

      // Call user's onMessage
      await this.callbacks.onMessage(content!, msg);

      // Acknowledge successful processing
      this.channel.ack(msg);
    } catch (err: unknown) {
      // Handle error
      const error = err instanceof Error ? err : new Error(String(err));

      try {
        await this.callbacks.onError(error, content, msg);
      } catch (handlerErr: unknown) {
        const handlerMessage =
          handlerErr instanceof Error ? handlerErr.message : String(handlerErr);
        this.callbacks.logger?.error(
          `[RabbitMQ] onError handler threw: ${handlerMessage}`,
        );
      }

      // Determine if message should be requeued
      // JSON parse errors should never be requeued
      const requeue = isParseError ? false : !this.useDLQ;
      this.channel.nack(msg, false, requeue);
    }
  }

  /**
   * Create message handler for consume
   */
  createHandler(): (msg: ConsumeMessage | null) => Promise<void> {
    return async (msg: ConsumeMessage | null) => {
      if (msg) {
        await this.processMessage(msg);
      }
    };
  }
}
