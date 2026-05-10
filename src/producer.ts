import { BaseRabbit } from "./baseRabbit.js";

export class Producer extends BaseRabbit {
  private assertedQueues = new Set<string>();

  private async ensureQueue(queue: string, durable: boolean): Promise<void> {
    if (this.assertedQueues.has(queue)) return;
    const channel = await this.getChannel();
    await channel.assertQueue(queue, { durable });
    this.assertedQueues.add(queue);
  }

  async publish<T>(
    queue: string,
    message: T,
    options: {
      durable?: boolean;
      persistent?: boolean;
    } = {},
  ): Promise<boolean> {
    const durable = options.durable ?? true;
    const persistent = options.persistent ?? true;

    await this.ensureQueue(queue, durable);

    const channel = await this.getChannel();

    return channel.sendToQueue(queue, Buffer.from(JSON.stringify(message)), {
      persistent,
    });
  }

  override async close(): Promise<void> {
    this.assertedQueues.clear();
    await super.close();
  }
}
