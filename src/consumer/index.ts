import type { ConsumeMessage } from "amqplib";
import { BaseRabbit, BaseRabbitOptions } from "../baseRabbit.js";
import {
  ConsumeOptions,
  ExchangeConsumeOptions,
  ExchangeType,
} from "../types.js";
import { BindingManager } from "./bindingManager.js";
import { MessageHandler } from "./messageHandler.js";
import { QueueSetup } from "./queueSetup.js";
import { RecoveryManager } from "./recoveryManager.js";

export abstract class Consumer<T> extends BaseRabbit {
  private queueSetup: QueueSetup;
  private bindingManager: BindingManager;
  private recoveryManager: RecoveryManager;
  private currentQueue?: string;
  private currentOptions?: ExchangeConsumeOptions;
  private isConsuming = false;

  constructor(
    url: string,
    options: BaseRabbitOptions & { maxRecoverRetries?: number } = {},
  ) {
    super(url, options);
    this.queueSetup = new QueueSetup();
    this.bindingManager = new BindingManager();
    this.recoveryManager = new RecoveryManager(
      { maxRecoverRetries: options.maxRecoverRetries ?? -1 },
      this.logger,
    );
  }

  // User must implement these
  abstract onMessage(data: T, originalMsg: ConsumeMessage): Promise<void>;

  async onError(
    error: Error,
    data?: T,
    originalMsg?: ConsumeMessage,
  ): Promise<void> {
    this.logger.error(`[RabbitMQ Consumer Error]: ${error.message}`);

    if (data !== undefined) {
      this.logger.info(
        `[RabbitMQ] Failed message data: ${JSON.stringify(data)}`,
      );
    }
    if (originalMsg?.properties?.correlationId) {
      this.logger.info(
        `[RabbitMQ] Correlation ID: ${originalMsg.properties.correlationId}`,
      );
    }
  }

  /**
   * Start consuming from a queue
   */
  async consume(
    queue: string,
    options: ExchangeConsumeOptions = {},
  ): Promise<void> {
    // Store for recovery
    this.currentQueue = queue;
    this.currentOptions = options;

    try {
      const channel = await this.getChannel();
      const prefetch = options.prefetch ?? 1;
      const useDLQ = options.useDLQ ?? false;

      // Set prefetch
      await channel.prefetch(prefetch);

      // Step 1: Setup queue (with DLQ if needed)
      await this.queueSetup.setupQueue(channel, queue, { useDLQ });

      // Step 2: Bind to exchange if provided
      if (options.exchange && options.exchangeType) {
        await this.bindingManager.bind(
          channel,
          queue,
          options.exchange,
          options.exchangeType,
          options.routingKey ?? "",
        );
      }

      // Step 3: Create message handler
      const messageHandler = new MessageHandler<T>(
        channel,
        {
          onMessage: this.onMessage.bind(this),
          onError: this.onError.bind(this),
          logger: this.logger,
        },
        useDLQ,
      );

      // Step 4: Start consuming
      await channel.consume(queue, messageHandler.createHandler());

      // Step 5: Set up recovery listeners
      channel.removeAllListeners("close");
      channel.removeAllListeners("error");
      channel.on("close", () => this.handleRecovery());
      channel.on("error", () => this.handleRecovery());

      // Reset recovery state on successful consume
      this.recoveryManager.reset();
      this.isConsuming = true;

      this.logger.info(`[RabbitMQ] Started consuming from queue: ${queue}`);
    } catch (err: unknown) {
      this.logger.error(
        `[RabbitMQ] Initial consumption failed for "${queue}", attempting recovery...`,
      );
      await this.handleRecovery();
    }
  }

  /**
   * Bind queue to an exchange at runtime
   */
  async bindQueue(
    queue: string,
    exchange: string,
    exchangeType: ExchangeType,
    routingKey?: string,
  ): Promise<void> {
    if (!this.currentQueue || this.currentQueue !== queue) {
      throw new Error(
        `Cannot bind: not currently consuming from queue "${queue}"`,
      );
    }

    const channel = await this.getChannel();
    await this.bindingManager.bind(
      channel,
      queue,
      exchange,
      exchangeType,
      routingKey ?? "",
    );

    this.logger.info(
      `[RabbitMQ] Bound queue "${queue}" to exchange "${exchange}" with routing key "${routingKey ?? ""}"`,
    );
  }

  /**
   * Unbind queue from an exchange
   */
  async unbindQueue(
    queue: string,
    exchange: string,
    routingKey?: string,
  ): Promise<void> {
    const channel = await this.getChannel();
    await this.bindingManager.unbind(
      channel,
      queue,
      exchange,
      routingKey ?? "",
    );

    this.logger.info(
      `[RabbitMQ] Unbound queue "${queue}" from exchange "${exchange}" with routing key "${routingKey ?? ""}"`,
    );
  }

  /**
   * Get currently consumed queue name
   */
  getCurrentQueue(): string | undefined {
    return this.currentQueue;
  }

  /**
   * Force recovery manually
   */
  async forceRecover(): Promise<void> {
    if (!this.currentQueue || !this.currentOptions) {
      this.logger.warn("[RabbitMQ] Cannot recover: no active consumption");
      return;
    }
    await this.handleRecovery();
  }

  /**
   * Handle recovery logic
   */
  private async handleRecovery(): Promise<void> {
    if (!this.recoveryManager.canRecover()) {
      return;
    }

    if (!this.currentQueue || !this.currentOptions) {
      this.logger.error(
        "[RabbitMQ] Cannot recover: no queue or options stored",
      );
      return;
    }

    this.recoveryManager.startRecovery();

    const delay = this.recoveryManager.getNextDelay();
    this.logger.warn(
      `[RabbitMQ] Consumer for "${this.currentQueue}" lost connection. Recovering in ${delay}ms... (attempt ${this.recoveryManager.getRetryCount()})`,
    );

    // Wait for backoff
    await new Promise((resolve) => setTimeout(resolve, delay));

    // Clean up old channel
    if (this.channel) {
      try {
        await this.channel.close();
      } catch {
        // ignore
      }
      this.channel = undefined;
    }

    // Reset recovery flag before re-consuming
    this.recoveryManager.completeRecovery();

    // Re-consume
    await this.consume(this.currentQueue, this.currentOptions);
  }

  /**
   * Close consumer and clean up
   */
  async close(): Promise<void> {
    this.isConsuming = false;
    this.bindingManager.clear();
    await super.close();
  }

  /**
   * Check if currently consuming
   */
  isActive(): boolean {
    return this.isConsuming;
  }

  /**
   * Get active bindings
   */
  getActiveBindings(): string[] {
    return this.bindingManager.getActiveBindings();
  }
}
