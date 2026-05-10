import type { ConsumeMessage } from "amqplib";
import { BaseRabbit } from "./baseRabbit.js";

export abstract class Consumer<T> extends BaseRabbit {
  abstract onMessage(data: T, originalMsg: ConsumeMessage): Promise<void>;

  async consume(
    queue: string,
    options: {
      durable?: boolean;
      prefetch?: number;
    } = {},
  ): Promise<void> {
    const channel = await this.getChannel();

    const prefetch = options.prefetch ?? 1;

    if (prefetch > 0) {
      await channel.prefetch(prefetch);
    }

    await channel.assertQueue(queue, {
      durable: options.durable ?? true,
    });

    await channel.consume(queue, async (msg) => {
      if (!msg) return;

      try {
        const content: T = JSON.parse(msg.content.toString());
        await this.onMessage(content, msg);
        channel.ack(msg);
      } catch (err) {
        console.error("[RabbitMQ] Processing error:", err);
        channel.nack(msg, false, false);
      }
    });
  }
}
