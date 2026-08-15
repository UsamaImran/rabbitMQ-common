import { BaseRabbit, type BaseRabbitOptions } from "./baseRabbit.js";
import { RabbitPublishError } from "./types.js";
import type {
  PublishOptions,
  QueueOptions,
  ExchangePublishOptions,
  ExchangeType,
  BatchPublishResult,
  BatchPublishError,
} from "./types.js";
import { ExchangeManager } from "./exchangeManager.js";

export class Producer extends BaseRabbit {
  // FIX #2: instance-level set so two Producer instances don't share state
  private assertedQueues = new Set<string>();
  private exchangeManager = new ExchangeManager();

  constructor(url: string, options: BaseRabbitOptions = {}) {
    super(url, options);
  }

  async publish<T>(
    queue: string,
    message: T,
    publishOptions: PublishOptions = {},
    queueOptions: QueueOptions = {},
  ): Promise<boolean> {
    try {
      const channel = await this.getChannel();

      if (!this.assertedQueues.has(queue)) {
        await channel.assertQueue(queue, {
          durable: queueOptions.durable ?? true,
          ...(queueOptions.maxLength && { maxLength: queueOptions.maxLength }),
          ...(queueOptions.messageTtl && {
            messageTtl: queueOptions.messageTtl,
          }),
          ...(queueOptions.priority && { maxPriority: queueOptions.priority }),
        });
        this.assertedQueues.add(queue);
      }

      return channel.sendToQueue(queue, Buffer.from(JSON.stringify(message)), {
        persistent: publishOptions.persistent ?? true,
        ...(publishOptions.expiration && {
          expiration: publishOptions.expiration,
        }),
        ...(publishOptions.priority !== undefined && {
          priority: publishOptions.priority,
        }),
      });
    } catch (err: unknown) {
      this.channel = undefined;
      const errorMessage = err instanceof Error ? err.message : String(err);
      throw new RabbitPublishError(
        `Failed to publish to queue "${queue}": ${errorMessage}`,
        queue,
        err,
      );
    }
  }

  /**
   * BATCH PUBLISH - Send multiple messages to the same queue in one operation
   *
   * @param queue - Queue name (all messages go to this queue)
   * @param messages - Array of messages to publish
   * @param publishOptions - Optional publish options (applied to all messages)
   * @param queueOptions - Optional queue assertion options
   * @returns Promise<BatchPublishResult> - Contains success/failure counts and errors
   */
  async publishBatch<T>(
    queue: string,
    messages: T[],
    publishOptions: PublishOptions = {},
    queueOptions: QueueOptions = {},
  ): Promise<BatchPublishResult> {
    // Early return for empty batch
    if (messages.length === 0) {
      return {
        total: 0,
        successful: 0,
        failed: 0,
        errors: [],
      };
    }

    try {
      const channel = await this.getChannel();

      // Assert queue (cached)
      if (!this.assertedQueues.has(queue)) {
        await channel.assertQueue(queue, {
          durable: queueOptions.durable ?? true,
          ...(queueOptions.maxLength && { maxLength: queueOptions.maxLength }),
          ...(queueOptions.messageTtl && {
            messageTtl: queueOptions.messageTtl,
          }),
          ...(queueOptions.priority && { maxPriority: queueOptions.priority }),
        });
        this.assertedQueues.add(queue);
      }

      const errors: BatchPublishError[] = [];

      // Publish all messages
      for (let i = 0; i < messages.length; i++) {
        try {
          const content = Buffer.from(JSON.stringify(messages[i]));
          const properties = {
            persistent: publishOptions.persistent ?? true,
            ...(publishOptions.expiration && {
              expiration: publishOptions.expiration,
            }),
            ...(publishOptions.priority !== undefined && {
              priority: publishOptions.priority,
            }),
          };

          const result = channel.sendToQueue(queue, content, properties);

          // If false, buffer is full - wait for drain and retry
          if (result === false) {
            await this.waitForDrain();
            // Retry once after drain
            const retryResult = channel.sendToQueue(queue, content, properties);
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

      const successful = messages.length - errors.length;

      if (errors.length > 0) {
        this.logger?.warn?.(
          `Batch publish: ${successful}/${messages.length} succeeded, ${errors.length} failed`,
        );
      } else {
        this.logger?.info?.(
          `Batch published ${successful}/${messages.length} messages to queue: ${queue}`,
        );
      }

      return {
        total: messages.length,
        successful,
        failed: errors.length,
        errors,
      };
    } catch (err: unknown) {
      this.channel = undefined;
      const errorMessage = err instanceof Error ? err.message : String(err);
      throw new RabbitPublishError(
        `Failed to publish batch to queue "${queue}": ${errorMessage}`,
        queue,
        err,
      );
    }
  }

  /**
   * BATCH PUBLISH TO EXCHANGE - Send multiple messages to the same exchange
   *
   * @param exchange - Exchange name (all messages go to this exchange)
   * @param type - Exchange type (fanout, topic, direct)
   * @param messages - Array of messages to publish
   * @param options - Optional publish options (applied to all messages)
   * @returns Promise<BatchPublishResult>
   */
  async publishBatchToExchange<T>(
    exchange: string,
    type: ExchangeType,
    messages: T[],
    options: ExchangePublishOptions = {},
  ): Promise<BatchPublishResult> {
    // Early return for empty batch
    if (messages.length === 0) {
      return {
        total: 0,
        successful: 0,
        failed: 0,
        errors: [],
      };
    }

    try {
      const channel = await this.getChannel();

      // Ensure exchange exists (cached)
      await this.exchangeManager.assertExchange(channel, exchange, type);

      const routingKey = options.routingKey ?? "";
      const errors: BatchPublishError[] = [];

      // Publish all messages
      for (let i = 0; i < messages.length; i++) {
        try {
          const content = Buffer.from(JSON.stringify(messages[i]));
          const properties = {
            persistent: options.persistent ?? true,
            ...(options.expiration && { expiration: options.expiration }),
            ...(options.priority !== undefined && {
              priority: options.priority,
            }),
          };

          const result = channel.publish(
            exchange,
            routingKey,
            content,
            properties,
          );

          // If false, buffer is full - wait for drain and retry
          if (result === false) {
            await this.waitForDrain();
            // Retry once after drain
            const retryResult = channel.publish(
              exchange,
              routingKey,
              content,
              properties,
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

      const successful = messages.length - errors.length;

      if (errors.length > 0) {
        this.logger?.warn?.(
          `Batch exchange publish: ${successful}/${messages.length} succeeded, ${errors.length} failed`,
        );
      } else {
        this.logger?.info?.(
          `Batch published ${successful}/${messages.length} messages to exchange: ${exchange}`,
        );
      }

      return {
        total: messages.length,
        successful,
        failed: errors.length,
        errors,
      };
    } catch (err: unknown) {
      this.channel = undefined;
      const errorMessage = err instanceof Error ? err.message : String(err);
      throw new RabbitPublishError(
        `Failed to publish batch to exchange "${exchange}": ${errorMessage}`,
        exchange,
        err,
      );
    }
  }

  /**
   * NEW: Publishes a message to an exchange.
   * @param exchange - Exchange name
   * @param type - Exchange type (fanout, topic, direct)
   * @param message - Message payload
   * @param options - Publish options (routingKey, persistent, etc.)
   */
  async publishToExchange<T>(
    exchange: string,
    type: ExchangeType,
    message: T,
    options: ExchangePublishOptions = {},
  ): Promise<boolean> {
    try {
      const channel = await this.getChannel();

      // Ensure exchange exists (cached)
      await this.exchangeManager.assertExchange(channel, exchange, type);

      // Default routingKey: for fanout exchanges, empty string is fine
      // For topic/direct, users should provide one
      const routingKey = options.routingKey ?? "";

      return channel.publish(
        exchange,
        routingKey,
        Buffer.from(JSON.stringify(message)),
        {
          persistent: options.persistent ?? true,
          ...(options.expiration && { expiration: options.expiration }),
          ...(options.priority !== undefined && { priority: options.priority }),
        },
      );
    } catch (err: unknown) {
      this.channel = undefined;
      const errorMessage = err instanceof Error ? err.message : String(err);
      throw new RabbitPublishError(
        `Failed to publish to exchange "${exchange}": ${errorMessage}`,
        exchange,
        err,
      );
    }
  }

  async waitForDrain(): Promise<void> {
    const channel = await this.getChannel();
    return new Promise((resolve) => {
      channel.once("drain", () => resolve());
    });
  }

  // Invalidate the asserted queue cache (useful after reconnection)
  resetQueueCache(queue?: string): void {
    if (queue) {
      this.assertedQueues.delete(queue);
    } else {
      this.assertedQueues.clear();
    }
  }

  resetExchangeCache(exchange?: string, type?: ExchangeType): void {
    this.exchangeManager.resetExchangeCache(exchange, type);
  }
}
