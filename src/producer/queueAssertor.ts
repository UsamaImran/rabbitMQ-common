import type { Channel } from "amqplib";
import type { QueueOptions } from "../types.js";

export class QueueAssertor {
  private channel?: Channel;
  private assertedQueues = new Map<string, string>();

  async assertQueue(
    channel: Channel,
    queue: string,
    options: QueueOptions = {},
  ): Promise<void> {
    this.ensureChannel(channel);

    const signature = this.signature(options);
    if (this.assertedQueues.get(queue) === signature) return;

    await channel.assertQueue(queue, {
      durable: options.durable ?? true,
      ...(options.maxLength !== undefined && { maxLength: options.maxLength }),
      ...(options.messageTtl !== undefined && { messageTtl: options.messageTtl }),
      ...(options.priority !== undefined && { maxPriority: options.priority }),
    });

    this.assertedQueues.set(queue, signature);
  }

  resetCache(queue?: string): void {
    if (queue) this.assertedQueues.delete(queue);
    else this.assertedQueues.clear();
  }

  isAsserted(queue: string): boolean {
    return this.assertedQueues.has(queue);
  }

  resetForChannel(channel: Channel): void {
    if (this.channel !== channel) {
      this.channel = channel;
      this.assertedQueues.clear();
    }
  }

  private ensureChannel(channel: Channel): void {
    this.resetForChannel(channel);
  }

  private signature(options: QueueOptions): string {
    return JSON.stringify({
      durable: options.durable ?? true,
      maxLength: options.maxLength,
      messageTtl: options.messageTtl,
      priority: options.priority,
    });
  }
}
