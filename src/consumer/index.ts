import type { Channel, ConsumeMessage } from "amqplib";
import { BaseRabbit, BaseRabbitOptions } from "../baseRabbit.js";
import { RabbitConsumeError } from "../types.js";
import type { ExchangeConsumeOptions, ExchangeType } from "../types.js";
import { BindingManager } from "./bindingManager.js";
import { MessageHandler } from "./messageHandler.js";
import { QueueSetup } from "./queueSetup.js";
import { RecoveryManager } from "./recoveryManager.js";

export interface ConsumerOptions extends BaseRabbitOptions {
  maxRecoverRetries?: number;
  backoffBase?: number;
  maxBackoff?: number;
  recoveryJitter?: number;
}

export abstract class Consumer<T> extends BaseRabbit {
  private readonly queueSetup = new QueueSetup();
  private readonly bindingManager = new BindingManager();
  private readonly recoveryManager: RecoveryManager;
  private currentQueue?: string;
  private currentOptions?: ExchangeConsumeOptions;
  private recoveryPromise?: Promise<void>;
  private closed = false;
  private generation = 0;
  private isConsuming = false;

  constructor(url: string, options: ConsumerOptions = {}) {
    super(url, options);
    this.recoveryManager = new RecoveryManager(
      {
        maxRecoverRetries: options.maxRecoverRetries ?? -1,
        backoffBase: options.backoffBase,
        maxBackoff: options.maxBackoff,
        jitter: options.recoveryJitter,
      },
      this.logger,
    );
  }

  abstract onMessage(data: T, originalMsg: ConsumeMessage): Promise<void>;

  async onError(error: Error, _data?: T, originalMsg?: ConsumeMessage): Promise<void> {
    this.logger.error(`[RabbitMQ Consumer Error]: ${error.message}`);
    if (originalMsg?.properties?.correlationId) {
      this.logger.info(`[RabbitMQ] Correlation ID: ${originalMsg.properties.correlationId}`);
    }
  }

  protected override onChannelInvalidated(channel: Channel): void {
    this.queueSetup.resetForChannel(channel);
    this.bindingManager.resetExchangeCache();
  }

  async consume(queue: string, options: ExchangeConsumeOptions = {}): Promise<void> {
    this.closed = false;
    this.currentQueue = queue;
    this.currentOptions = { ...options };
    this.isConsuming = false;
    this.bindingManager.clear();

    try {
      await this.startConsumption(queue, options, false);
    } catch (err: unknown) {
      const error = err instanceof Error ? err : new Error(String(err));
      this.isConsuming = false;
      throw new RabbitConsumeError(
        `Failed to start consuming from queue "${queue}": ${error.message}`,
        queue,
        err,
      );
    }
  }

  private async startConsumption(
    queue: string,
    options: ExchangeConsumeOptions,
    recovering: boolean,
  ): Promise<void> {
    const channel = await this.getChannel();
    const prefetch = options.prefetch ?? 1;

    await channel.prefetch(prefetch);
    await this.queueSetup.setupQueue(channel, queue, {
      useDLQ: this.useDLQ,
      queueOptions: this.queueOptions,
    });

    if (recovering) {
      await this.bindingManager.restore(channel);
    } else if (options.exchange && options.exchangeType) {
      await this.bindingManager.bind(
        channel,
        queue,
        options.exchange,
        options.exchangeType,
        options.routingKey ?? "",
      );
    }

    const messageHandler = new MessageHandler<T>(
      channel,
      {
        onMessage: this.onMessage.bind(this),
        onError: this.onError.bind(this),
        logger: this.logger,
      },
      this.useDLQ,
    );

    await channel.consume(queue, messageHandler.createHandler());

    const generation = ++this.generation;
    channel.on("close", () => {
      if (!this.closed && generation === this.generation) void this.handleRecovery();
    });
    channel.on("error", () => {
      if (!this.closed && generation === this.generation) void this.handleRecovery();
    });

    this.recoveryManager.reset();
    this.isConsuming = true;
    this.logger.info(`[RabbitMQ] Started consuming from queue: ${queue}`);
  }

  async bindQueue(
    queue: string,
    exchange: string,
    exchangeType: ExchangeType,
    routingKey = "",
  ): Promise<void> {
    if (!this.currentQueue || this.currentQueue !== queue || !this.isConsuming) {
      throw new Error(`Cannot bind: not currently consuming from queue "${queue}"`);
    }

    const channel = await this.getChannel();
    await this.bindingManager.bind(channel, queue, exchange, exchangeType, routingKey);
  }

  async unbindQueue(queue: string, exchange: string, routingKey = ""): Promise<void> {
    if (!this.currentQueue || this.currentQueue !== queue || !this.isConsuming) {
      throw new Error(`Cannot unbind: not currently consuming from queue "${queue}"`);
    }

    const channel = await this.getChannel();
    await this.bindingManager.unbind(channel, queue, exchange, routingKey);
  }

  getCurrentQueue(): string | undefined {
    return this.currentQueue;
  }

  async forceRecover(): Promise<void> {
    if (!this.currentQueue || !this.currentOptions || !this.isConsuming) {
      this.logger.warn("[RabbitMQ] Cannot recover: no active consumption");
      return;
    }
    await this.handleRecovery();
  }

  private async handleRecovery(): Promise<void> {
    if (this.closed || !this.currentQueue || !this.currentOptions) return;
    if (this.recoveryPromise) return this.recoveryPromise;

    this.recoveryPromise = this.recoverLoop(this.currentQueue, this.currentOptions)
      .finally(() => {
        this.recoveryPromise = undefined;
      });
    return this.recoveryPromise;
  }

  private async recoverLoop(queue: string, options: ExchangeConsumeOptions): Promise<void> {
    while (!this.closed && this.recoveryManager.canRecover()) {
      this.recoveryManager.startRecovery();
      const delay = this.recoveryManager.getNextDelay();
      this.logger.warn(
        `[RabbitMQ] Consumer for "${queue}" lost connection. Recovering in ${delay}ms... (attempt ${this.recoveryManager.getRetryCount()})`,
      );
      await new Promise((resolve) => setTimeout(resolve, delay));
      if (this.closed) return;

      try {
        await super.close();
        await this.startConsumption(queue, options, true);
        return;
      } catch (err: unknown) {
        this.isConsuming = false;
        this.logger.error(
          `[RabbitMQ] Recovery attempt failed: ${err instanceof Error ? err.message : String(err)}`,
        );
      }
    }

    if (!this.closed) {
      this.logger.error(`[RabbitMQ] Recovery exhausted for queue "${queue}"`);
    }
  }

  async close(): Promise<void> {
    this.closed = true;
    this.isConsuming = false;
    this.generation++;
    this.bindingManager.clear();
    this.currentQueue = undefined;
    this.currentOptions = undefined;
    await super.close();
  }

  isActive(): boolean {
    return this.isConsuming;
  }

  getActiveBindings(): string[] {
    return this.bindingManager.getActiveBindings();
  }
}
