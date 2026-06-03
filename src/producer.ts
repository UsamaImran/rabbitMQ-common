import { BaseRabbit, type BaseRabbitOptions } from "./baseRabbit.js";
import { RabbitPublishError } from "./types.js";
import type {
  PublishOptions,
  QueueOptions,
  ExchangePublishOptions,
  ExchangeType,
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
