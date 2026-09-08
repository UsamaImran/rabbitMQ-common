import type { Channel } from "amqplib";
import type { QueueOptions } from "../types.js";

export class QueueAssertor {
  private channel?: Channel;
  private assertedQueues = new Map<string, string>();

  async assertQueue(channel: Channel, queue: string, options: QueueOptions = {}, useDLQ = false): Promise<void> {
    this.resetForChannel(channel);
    const signature = this.signature(options, useDLQ);
    if (this.assertedQueues.get(queue) === signature) return;

    const queueArguments = this.queueArguments(options);
    if (useDLQ) {
      const dlx = `${queue}_dlx`;
      const dlq = `${queue}_failed`;
      await channel.assertExchange(dlx, "direct", { durable: true });
      await channel.assertQueue(dlq, {
        durable: options.durable ?? true,
        ...(options.maxLength !== undefined && { maxLength: options.maxLength }),
        ...(options.messageTtl !== undefined && { messageTtl: options.messageTtl }),
      });
      await channel.bindQueue(dlq, dlx, "dead-letter");
      await channel.assertQueue(queue, {
        ...queueArguments,
        deadLetterExchange: dlx,
        deadLetterRoutingKey: "dead-letter",
      });
    } else {
      await channel.assertQueue(queue, queueArguments);
    }
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

  invalidateChannel(channel: Channel): void {
    if (this.channel === channel) this.channel = undefined;
    this.assertedQueues.clear();
  }

  private queueArguments(options: QueueOptions) {
    return {
      durable: options.durable ?? true,
      ...(options.maxLength !== undefined && { maxLength: options.maxLength }),
      ...(options.messageTtl !== undefined && { messageTtl: options.messageTtl }),
      ...(options.priority !== undefined && { maxPriority: options.priority }),
    };
  }

  private signature(options: QueueOptions, useDLQ: boolean): string {
    return JSON.stringify({ ...this.queueArguments(options), useDLQ });
  }
}
