import { BaseRabbit, BaseRabbitOptions } from "../baseRabbit.js";
import { ExchangeManager } from "../exchangeManager.js";
import { RabbitPublishError } from "../types.js";
import type {
  PublishOptions,
  QueueOptions,
  ExchangePublishOptions,
  ExchangeType,
  BatchPublishResult,
} from "../types.js";
import { BatchHandler } from "./batchHandler.js";
import { MessageSender } from "./messageSender.js";
import { QueueAssertor } from "./queueAssertor.js";

export class Producer extends BaseRabbit {
  private queueAssertor: QueueAssertor;
  private batchHandler: BatchHandler;
  private messageSender: MessageSender;
  private exchangeManager: ExchangeManager;

  constructor(url: string, options: BaseRabbitOptions = {}) {
    super(url, options);
    this.queueAssertor = new QueueAssertor();
    this.batchHandler = new BatchHandler();
    this.messageSender = new MessageSender();
    this.exchangeManager = new ExchangeManager();
  }

  /**
   * Publish a single message to a queue
   */
  async publish<T>(
    queue: string,
    message: T,
    publishOptions: PublishOptions = {},
    queueOptions: QueueOptions = {},
  ): Promise<boolean> {
    try {
      const channel = await this.getChannel();

      // Delegate queue assertion
      await this.queueAssertor.assertQueue(channel, queue, queueOptions);

      // Delegate message sending
      return this.messageSender.sendToQueue(
        channel,
        queue,
        message,
        publishOptions,
      );
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
   * Publish a single message to an exchange
   */
  async publishToExchange<T>(
    exchange: string,
    type: ExchangeType,
    message: T,
    options: ExchangePublishOptions = {},
  ): Promise<boolean> {
    try {
      const channel = await this.getChannel();

      // Delegate exchange assertion
      await this.exchangeManager.assertExchange(channel, exchange, type);

      const routingKey = options.routingKey ?? "";

      // Delegate message sending
      return this.messageSender.publishToExchange(
        channel,
        exchange,
        routingKey,
        message,
        options,
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

  /**
   * Batch publish multiple messages to a queue
   */
  async publishBatch<T>(
    queue: string,
    messages: T[],
    publishOptions: PublishOptions = {},
    queueOptions: QueueOptions = {},
  ): Promise<BatchPublishResult> {
    try {
      const channel = await this.getChannel();

      // Delegate queue assertion (once for the batch)
      await this.queueAssertor.assertQueue(channel, queue, queueOptions);

      // Delegate batch publishing
      const result = await this.batchHandler.publishBatch(
        channel,
        queue,
        messages,
        publishOptions,
        () => this.waitForDrain(),
      );

      // Log results
      if (result.failed > 0) {
        this.logger?.warn?.(
          `Batch publish: ${result.successful}/${result.total} succeeded, ${result.failed} failed`,
        );
      } else if (result.total > 0) {
        this.logger?.info?.(
          `Batch published ${result.total} messages to queue: ${queue}`,
        );
      }

      return result;
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
   * Batch publish multiple messages to an exchange
   */
  async publishBatchToExchange<T>(
    exchange: string,
    type: ExchangeType,
    messages: T[],
    options: ExchangePublishOptions = {},
  ): Promise<BatchPublishResult> {
    try {
      const channel = await this.getChannel();

      // Delegate exchange assertion (once for the batch)
      await this.exchangeManager.assertExchange(channel, exchange, type);

      const routingKey = options.routingKey ?? "";

      // Delegate batch publishing
      const result = await this.batchHandler.publishBatchToExchange(
        channel,
        exchange,
        routingKey,
        messages,
        options,
        () => this.waitForDrain(),
      );

      // Log results
      if (result.failed > 0) {
        this.logger?.warn?.(
          `Batch exchange publish: ${result.successful}/${result.total} succeeded, ${result.failed} failed`,
        );
      } else if (result.total > 0) {
        this.logger?.info?.(
          `Batch published ${result.total} messages to exchange: ${exchange}`,
        );
      }

      return result;
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
   * Wait for the channel's write buffer to drain
   */
  async waitForDrain(): Promise<void> {
    const channel = await this.getChannel();
    return new Promise((resolve) => {
      channel.once("drain", () => resolve());
    });
  }

  /**
   * Reset queue assertion cache
   */
  resetQueueCache(queue?: string): void {
    this.queueAssertor.resetCache(queue);
  }

  /**
   * Reset exchange assertion cache
   */
  resetExchangeCache(exchange?: string, type?: ExchangeType): void {
    this.exchangeManager.resetExchangeCache(exchange, type);
  }
}
