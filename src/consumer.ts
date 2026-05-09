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
  ) {
    if (!this.channel) {
      await this.init();
    }

    if (options.prefetch) {
      await this.channel!.prefetch(options.prefetch);
    }

    await this.channel!.assertQueue(queue, {
      durable: options.durable ?? true,
    });

    this.channel!.consume(queue, async (msg) => {
      if (!msg) return;

      try {
        const content: T = JSON.parse(msg.content.toString());

        await this.onMessage(content, msg);

        this.channel!.ack(msg);
      } catch (err) {
        console.error("[RabbitMQ] Processing error:", err);

        this.channel!.nack(msg, false, false);
      }
    });
  }
}
