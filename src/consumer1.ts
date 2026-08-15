import type { ConsumeMessage } from "amqplib";
import { BaseRabbit, type BaseRabbitOptions } from "./baseRabbit.js";
import type { ConsumeOptions, ExchangeType } from "./types.js";
import { ExchangeManager } from "./exchangeManager.js";

export interface ExchangeConsumeOptions extends ConsumeOptions {
  exchange?: string;
  exchangeType?: ExchangeType;
  routingKey?: string;
}

export abstract class Consumer<T> extends BaseRabbit {
  private isRecovering = false;
  private recoverRetries = 0;
  private readonly maxRecoverRetries: number;
  private currentQueue?: string;
  private currentOptions?: ExchangeConsumeOptions;
  private exchangeManager = new ExchangeManager();
  private bindings = new Set<string>(); // Track active bindings for cleanup

  constructor(
    url: string,
    options: BaseRabbitOptions & { maxRecoverRetries?: number } = {},
  ) {
    super(url, options);
    this.maxRecoverRetries = options.maxRecoverRetries ?? -1;
  }

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
      const exchange = options.exchange;
      const exchangeType = options.exchangeType;
      const routingKey = options.routingKey;

      await channel.prefetch(prefetch);

      // Step 1: Assert queue with DLQ if needed
      if (useDLQ) {
        const dlx = `${queue}_dlx`;
        const dlq = `${queue}_failed`;
        await channel.assertExchange(dlx, "direct");
        await channel.assertQueue(dlq, { durable: true });
        await channel.bindQueue(dlq, dlx, "dead-letter");
        await channel.assertQueue(queue, {
          durable: true,
          deadLetterExchange: dlx,
          deadLetterRoutingKey: "dead-letter",
        });
      } else {
        await channel.assertQueue(queue, { durable: true });
      }

      // Step 2: Bind to exchange if provided
      if (exchange && exchangeType) {
        await this.exchangeManager.assertExchange(
          channel,
          exchange,
          exchangeType,
        );
        await channel.bindQueue(queue, exchange, routingKey ?? "");

        // Track binding for cleanup
        const bindingKey = `${exchange}:${routingKey ?? ""}`;
        this.bindings.add(bindingKey);
      }

      // Step 3: Start consuming
      await channel.consume(queue, async (msg) => {
        if (!msg) return;
        let content: T | undefined;
        let isParseError = false;

        try {
          try {
            content = JSON.parse(msg.content.toString()) as T;
          } catch (parseErr: unknown) {
            isParseError = true;
            const message =
              parseErr instanceof Error ? parseErr.message : String(parseErr);
            throw new Error(`Failed to parse message: ${message}`);
          }

          await this.onMessage(content!, msg);
          channel.ack(msg);
        } catch (err: unknown) {
          const error = err instanceof Error ? err : new Error(String(err));
          try {
            await this.onError(error, content, msg);
          } catch (handlerErr: unknown) {
            const handlerMessage =
              handlerErr instanceof Error
                ? handlerErr.message
                : String(handlerErr);
            this.logger.error(
              `[RabbitMQ] onError handler threw: ${handlerMessage}`,
            );
          }

          const requeue = isParseError ? false : !useDLQ;
          channel.nack(msg, false, requeue);
        }
      });

      // FIX #3: remove old listeners before adding new ones
      channel.removeAllListeners("close");
      channel.removeAllListeners("error");
      channel.on("close", () => this.recover());
      channel.on("error", () => this.recover());

      this.recoverRetries = 0;
    } catch (err: unknown) {
      this.logger.error(
        `[RabbitMQ] Initial consumption failed for "${queue}", attempting recovery...`,
      );
      await this.recover();
    }
  }

  async bindQueue(
    queue: string,
    exchange: string,
    routingKey?: string,
  ): Promise<void> {
    if (!this.currentQueue || this.currentQueue !== queue) {
      throw new Error(
        `Cannot bind: not currently consuming from queue "${queue}"`,
      );
    }

    const channel = await this.getChannel();

    // Need exchange type to assert — for bind-only, we need the type
    // This requires the exchange to already exist or the user to provide type
    // Simpler: assume exchange exists (declared elsewhere or during consume)
    await channel.bindQueue(queue, exchange, routingKey ?? "");

    const bindingKey = `${exchange}:${routingKey ?? ""}`;
    this.bindings.add(bindingKey);

    this.logger.info(
      `[RabbitMQ] Bound queue "${queue}" to exchange "${exchange}" with routing key "${routingKey ?? ""}"`,
    );
  }

  async unbindQueue(
    queue: string,
    exchange: string,
    routingKey?: string,
  ): Promise<void> {
    const channel = await this.getChannel();
    await channel.unbindQueue(queue, exchange, routingKey ?? "");

    const bindingKey = `${exchange}:${routingKey ?? ""}`;
    this.bindings.delete(bindingKey);
  }

  getCurrentQueue(): string | undefined {
    return this.currentQueue;
  }

  async forceRecover(): Promise<void> {
    if (!this.currentQueue || !this.currentOptions) {
      this.logger.warn("[RabbitMQ] Cannot recover: no active consumption");
      return;
    }
    await this.recover();
  }

  private async recover(): Promise<void> {
    if (this.isRecovering) return;

    if (!this.currentQueue || !this.currentOptions) {
      this.logger.error(
        "[RabbitMQ] Cannot recover: no queue or options stored",
      );
      return;
    }

    if (
      this.maxRecoverRetries !== -1 &&
      this.recoverRetries >= this.maxRecoverRetries
    ) {
      this.logger.error(
        `[RabbitMQ] Consumer for "${this.currentQueue}" failed to recover after ${this.recoverRetries} attempts. Giving up.`,
      );
      return;
    }

    // Clean up old channel
    if (this.channel) {
      try {
        await this.channel.close();
      } catch {
        // ignore
      }
      this.channel = undefined;
    }

    this.isRecovering = true;
    this.recoverRetries++;

    const delay = Math.min(Math.pow(2, this.recoverRetries) * 1000, 30000);
    this.logger.warn(
      `[RabbitMQ] Consumer for "${this.currentQueue}" lost connection. Recovering in ${delay}ms... (attempt ${this.recoverRetries})`,
    );

    await new Promise((resolve) => setTimeout(resolve, delay));
    this.isRecovering = false;

    return this.consume(this.currentQueue, this.currentOptions);
  }

  async close(): Promise<void> {
    // Clean up bindings before closing
    this.bindings.clear();
    await super.close();
  }
}
