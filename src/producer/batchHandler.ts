import type { Channel } from "amqplib";
import type {
  PublishOptions,
  ExchangePublishOptions,
  BatchPublishResult,
  BatchPublishError,
} from "../types.js";
import { MessageSender } from "./messageSender.js";

/**
 * Responsible for batch publishing logic
 */
export class BatchHandler {
  private sender: MessageSender;

  constructor() {
    this.sender = new MessageSender();
  }

  /**
   * Publish multiple messages to a queue
   */
  async publishBatch<T>(
    channel: Channel,
    queue: string,
    messages: T[],
    options: PublishOptions = {},
    waitForDrain: () => Promise<void>,
  ): Promise<BatchPublishResult> {
    if (messages.length === 0) {
      return this.emptyResult();
    }

    const errors: BatchPublishError[] = [];

    for (let i = 0; i < messages.length; i++) {
      try {
        const result = this.sender.sendToQueue(
          channel,
          queue,
          messages[i],
          options,
        );

        if (result === false) {
          await waitForDrain();
          const retryResult = this.sender.sendToQueue(
            channel,
            queue,
            messages[i],
            options,
          );
          if (retryResult === false) {
            throw new Error("Buffer still full after waiting for drain");
          }
        }
      } catch (err) {
        errors.push({
          index: i,
          message: messages[i],
          error: err instanceof Error ? err : new Error(String(err)),
        });
      }
    }

    return this.buildResult(messages.length, errors);
  }

  /**
   * Publish multiple messages to an exchange
   */
  async publishBatchToExchange<T>(
    channel: Channel,
    exchange: string,
    routingKey: string,
    messages: T[],
    options: ExchangePublishOptions = {},
    waitForDrain: () => Promise<void>,
  ): Promise<BatchPublishResult> {
    if (messages.length === 0) {
      return this.emptyResult();
    }

    const errors: BatchPublishError[] = [];

    for (let i = 0; i < messages.length; i++) {
      try {
        const result = this.sender.publishToExchange(
          channel,
          exchange,
          routingKey,
          messages[i],
          options,
        );

        if (result === false) {
          await waitForDrain();
          const retryResult = this.sender.publishToExchange(
            channel,
            exchange,
            routingKey,
            messages[i],
            options,
          );
          if (retryResult === false) {
            throw new Error("Buffer still full after waiting for drain");
          }
        }
      } catch (err) {
        errors.push({
          index: i,
          message: messages[i],
          error: err instanceof Error ? err : new Error(String(err)),
        });
      }
    }

    return this.buildResult(messages.length, errors);
  }

  private emptyResult(): BatchPublishResult {
    return {
      total: 0,
      successful: 0,
      failed: 0,
      errors: [],
    };
  }

  private buildResult(
    total: number,
    errors: BatchPublishError[],
  ): BatchPublishResult {
    return {
      total,
      successful: total - errors.length,
      failed: errors.length,
      errors,
    };
  }
}
