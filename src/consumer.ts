import { ConsumeMessage } from "amqplib";
import { BaseRabbit } from "./baseRabbit.js";

export abstract class Consumer<T> extends BaseRabbit {
  abstract onMessage(data: T, originalMsg: ConsumeMessage): void;

  async consume(queue: string, options: { durable?: boolean } = {}) {
    if (!this.channel) await this.init();

    await this.channel!.assertQueue(queue, {
      durable: options.durable ?? true,
    });

    this.channel!.consume(queue, (msg) => {
      if (msg) {
        try {
          const content: T = JSON.parse(msg.content.toString());
          this.onMessage(content, msg);
          this.channel!.ack(msg);
        } catch (err) {
          console.error("[RabbitMQ] Processing error:", err);
          this.channel!.nack(msg, false, false);
        }
      }
    });
  }
}
