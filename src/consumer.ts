import type { ConsumeMessage } from "amqplib";
import { BaseRabbit, type BaseRabbitOptions } from "./baseRabbit.js";
import type { ConsumeOptions } from "./types.js";
import { RabbitConsumeError } from "./types.js";

export abstract class Consumer<T> extends BaseRabbit {
  private isRecovering = false;
  private recoverRetries = 0;
  // FIX #5: respect maxRetries in recovery; default -1 = infinite (opt-in)
  private readonly maxRecoverRetries: number;
  private currentQueue?: string;
  private currentOptions?: ConsumeOptions;

  constructor(
    url: string,
    options: BaseRabbitOptions & { maxRecoverRetries?: number } = {},
  ) {
    super(url, options);
    this.maxRecoverRetries = options.maxRecoverRetries ?? -1;
  }

  /**
   * Called for every incoming message.
   * @param data - Parsed message payload
   * @param originalMsg - Raw amqplib message
   */
  abstract onMessage(data: T, originalMsg: ConsumeMessage): Promise<void>;

  async onError(
    error: Error,
    data?: T,
    originalMsg?: ConsumeMessage,
  ): Promise<void> {
    this.logger.error(`[RabbitMQ Consumer Error]: ${error.message}`);

    // Log debug info if available (helps with troubleshooting)
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
   * Starts consuming messages from a queue.
   * Automatically recovers on channel or connection loss.
   */
  async consume(
    queue: string,
    // FIX #10: typed options instead of `any`
    options: ConsumeOptions = {},
  ): Promise<void> {
    // Store for recovery
    this.currentQueue = queue;
    this.currentOptions = options;

    try {
      const channel = await this.getChannel();
      const prefetch = options.prefetch ?? 1;
      const useDLQ = options.useDLQ ?? false;

      await channel.prefetch(prefetch);

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

      await channel.consume(queue, async (msg) => {
        if (!msg) return;
        let content: T | undefined;
        let isParseError = false;

        try {
          // FIX #6: distinguish JSON parse errors from handler errors
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
          // FIX #7: await onError so errors in it aren't silently swallowed
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

          // FIX #6: never requeue malformed messages — they'll loop forever
          const requeue = isParseError ? false : !useDLQ;
          channel.nack(msg, false, requeue);
        }
      });

      // FIX #3: remove old listeners before adding new ones to prevent stacking
      channel.removeAllListeners("close");
      channel.removeAllListeners("error");
      channel.on("close", () => this.recover());
      channel.on("error", () => this.recover());

      // Reset retry counter on successful consumption
      this.recoverRetries = 0;
    } catch (err: unknown) {
      this.logger.error(
        `[RabbitMQ] Initial consumption failed for "${queue}", attempting recovery...`,
      );
      await this.recover();
    }
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

  // FIX #5: exponential backoff + retry limit in recovery
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

    // FIX #11: Clean up old channel before retrying to prevent memory leaks
    if (this.channel) {
      try {
        await this.channel.close();
      } catch {
        // ignore — channel is likely already dead
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

    // Recursive call to restart consumption
    return this.consume(this.currentQueue, this.currentOptions);
  }
}
