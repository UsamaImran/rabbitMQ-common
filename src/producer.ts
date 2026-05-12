import { BaseRabbit } from "./baseRabbit.js";

export class Producer extends BaseRabbit {
  private static assertedQueues = new Set<string>();

  async publish<T>(queue: string, message: T): Promise<boolean> {
    const channel = await this.getChannel();

    if (!Producer.assertedQueues.has(queue)) {
      await channel.assertQueue(queue, { durable: true });
      Producer.assertedQueues.add(queue);
    }

    return channel.sendToQueue(queue, Buffer.from(JSON.stringify(message)), {
      persistent: true,
    });
  }
}
