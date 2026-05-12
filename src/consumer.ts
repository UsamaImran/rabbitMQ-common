import type { ConsumeMessage } from "amqplib";
import { BaseRabbit } from "./baseRabbit.js";

export abstract class Consumer<T> extends BaseRabbit {
  private isRecovering = false;

  abstract onMessage(data: T, originalMsg: ConsumeMessage): Promise<void>;

  async onError(
    error: Error,
    data?: T,
    originalMsg?: ConsumeMessage,
  ): Promise<void> {
    console.error(`[RabbitMQ Consumer Error]:`, error.message);
  }

  async consume(
    queue: string,
    options: { prefetch?: number; useDLQ?: boolean } = {},
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
        try {
          content = JSON.parse(msg.content.toString());
          await this.onMessage(content!, msg);
          channel.ack(msg);
        } catch (err: any) {
          await this.onError(err, content, msg);
          channel.nack(msg, false, !useDLQ);
        }
      });

      channel.on("close", () => this.recover(queue, options));
      channel.on("error", () => this.recover(queue, options));
    } catch (err) {
      await this.recover(queue, options);
    }
  }

  private async recover(queue: string, options: any) {
    if (this.isRecovering) return;
    this.isRecovering = true;

    console.warn(
      `[RabbitMQ] Consumer for ${queue} lost connection. Recovering...`,
    );
    await new Promise((res) => setTimeout(res, 5000));

    this.isRecovering = false;
    return this.consume(queue, options);
  }
}
