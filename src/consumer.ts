import type { ConsumeMessage } from "amqplib";
import { BaseRabbit, type BaseRabbitOptions } from "./baseRabbit.js";
import type { ConsumeOptions } from "./types.js";

export abstract class Consumer<T> extends BaseRabbit {
  private isRecovering = false;
  private recoverRetries = 0;
  // FIX #5: respect maxRetries in recovery; default -1 = infinite (opt-in)
  private readonly maxRecoverRetries: number;

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
  }

  async consume(
    queue: string,
    // FIX #10: typed options instead of `any`
    options: ConsumeOptions = {},
  ): Promise<void> {
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
            content = JSON.parse(msg.content.toString());
          } catch (parseErr: any) {
            isParseError = true;
            throw new Error(`Failed to parse message: ${parseErr.message}`);
          }

          await this.onMessage(content!, msg);
          channel.ack(msg);
        } catch (err: any) {
          // FIX #7: await onError so errors in it aren't silently swallowed
          try {
            await this.onError(err, content, msg);
          } catch (handlerErr: any) {
            this.logger.error(
              `[RabbitMQ] onError handler threw: ${handlerErr.message}`,
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
      channel.on("close", () => this.recover(queue, options));
      channel.on("error", () => this.recover(queue, options));

      // Reset retry counter on successful connection
      this.recoverRetries = 0;
    } catch (err) {
      await this.recover(queue, options);
    }
  }

  // FIX #5: exponential backoff + retry limit in recovery
  private async recover(queue: string, options: ConsumeOptions): Promise<void> {
    if (this.isRecovering) return;

    if (
      this.maxRecoverRetries !== -1 &&
      this.recoverRetries >= this.maxRecoverRetries
    ) {
      this.logger.error(
        `[RabbitMQ] Consumer for "${queue}" failed to recover after ${this.recoverRetries} attempts. Giving up.`,
      );
      return;
    }

    this.isRecovering = true;
    this.recoverRetries++;

    const delay = Math.min(Math.pow(2, this.recoverRetries) * 1000, 30000);
    this.logger.warn(
      `[RabbitMQ] Consumer for "${queue}" lost connection. Recovering in ${delay}ms... (attempt ${this.recoverRetries})`,
    );

    await new Promise((res) => setTimeout(res, delay));
    this.isRecovering = false;

    return this.consume(queue, options);
  }
}
