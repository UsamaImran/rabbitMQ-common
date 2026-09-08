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
  private readonly queueAssertor = new QueueAssertor();
  private readonly batchHandler = new BatchHandler();
  private readonly messageSender = new MessageSender();
  private readonly exchangeManager = new ExchangeManager();

  constructor(url: string, options: BaseRabbitOptions = {}) {
    super(url, options);
  }

  protected override onChannelInvalidated(channel: import("amqplib").Channel): void {
    this.queueAssertor.resetForChannel(channel);
    this.exchangeManager.resetForChannel(channel);
  }

  async publish<T>(
    queue: string,
    message: T,
    publishOptions: PublishOptions = {},
    queueOptions: QueueOptions = {},
  ): Promise<boolean> {
    try {
      const channel = await this.getChannel();
      await this.queueAssertor.assertQueue(
        channel,
        queue,
        { ...this.queueOptions, ...queueOptions },
        this.useDLQ,
      );
      return this.messageSender.sendToQueue(channel, queue, message, publishOptions);
    } catch (err: unknown) {
      const errorMessage = err instanceof Error ? err.message : String(err);
      throw new RabbitPublishError(
        `Failed to publish to queue "${queue}": ${errorMessage}`,
        queue,
        err,
      );
    }
  }

  async publishToExchange<T>(
    exchange: string,
    type: ExchangeType,
    message: T,
    options: ExchangePublishOptions = {},
  ): Promise<boolean> {
    try {
      const channel = await this.getChannel();
      await this.exchangeManager.assertExchange(channel, exchange, type);
      return this.messageSender.publishToExchange(
        channel,
        exchange,
        options.routingKey ?? "",
        message,
        options,
      );
    } catch (err: unknown) {
      const errorMessage = err instanceof Error ? err.message : String(err);
      throw new RabbitPublishError(
        `Failed to publish to exchange "${exchange}": ${errorMessage}`,
        exchange,
        err,
      );
    }
  }

  async publishBatch<T>(
    queue: string,
    messages: T[],
    publishOptions: PublishOptions = {},
    queueOptions: QueueOptions = {},
  ): Promise<BatchPublishResult> {
    try {
      const channel = await this.getChannel();
      await this.queueAssertor.assertQueue(
        channel,
        queue,
        { ...this.queueOptions, ...queueOptions },
        this.useDLQ,
      );
      return await this.batchHandler.publishBatch(
        channel,
        queue,
        messages,
        publishOptions,
        (currentChannel) => this.waitForDrain(currentChannel),
      );
    } catch (err: unknown) {
      const errorMessage = err instanceof Error ? err.message : String(err);
      throw new RabbitPublishError(
        `Failed to publish batch to queue "${queue}": ${errorMessage}`,
        queue,
        err,
      );
    }
  }

  async publishBatchToExchange<T>(
    exchange: string,
    type: ExchangeType,
    messages: T[],
    options: ExchangePublishOptions = {},
  ): Promise<BatchPublishResult> {
    try {
      const channel = await this.getChannel();
      await this.exchangeManager.assertExchange(channel, exchange, type);
      return await this.batchHandler.publishBatchToExchange(
        channel,
        exchange,
        options.routingKey ?? "",
        messages,
        options,
        (currentChannel) => this.waitForDrain(currentChannel),
      );
    } catch (err: unknown) {
      const errorMessage = err instanceof Error ? err.message : String(err);
      throw new RabbitPublishError(
        `Failed to publish batch to exchange "${exchange}": ${errorMessage}`,
        exchange,
        err,
      );
    }
  }

  async waitForDrain(channel?: import("amqplib").Channel): Promise<void> {
    const currentChannel = channel ?? this.channel;
    if (!currentChannel) throw new Error("RabbitMQ channel is not available");

    return new Promise<void>((resolve, reject) => {
      const cleanup = () => {
        currentChannel.removeListener("drain", onDrain);
        currentChannel.removeListener("error", onError);
        currentChannel.removeListener("close", onClose);
      };
      const onDrain = () => { cleanup(); resolve(); };
      const onError = (error: Error) => { cleanup(); reject(error); };
      const onClose = () => { cleanup(); reject(new Error("RabbitMQ channel closed while waiting for drain")); };

      currentChannel.once("drain", onDrain);
      currentChannel.once("error", onError);
      currentChannel.once("close", onClose);
    });
  }

  resetQueueCache(queue?: string): void {
    this.queueAssertor.resetCache(queue);
  }

  resetExchangeCache(exchange?: string, type?: ExchangeType): void {
    this.exchangeManager.resetExchangeCache(exchange, type);
  }
}
