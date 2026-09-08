import type { Channel } from "amqplib";
import type { QueueOptions } from "../types.js";

export interface QueueSetupOptions {
  useDLQ?: boolean;
  queueOptions?: QueueOptions;
}

export class QueueSetup {
  private channel?: Channel;
  private assertedQueues = new Map<string, string>();

  async setupQueue(channel: Channel, queue: string, options: QueueSetupOptions = {}): Promise<void> {
    this.resetForChannel(channel);
    const useDLQ = options.useDLQ ?? false;
    const queueOptions = options.queueOptions ?? {};
    const signature = this.signature(queueOptions, useDLQ);
    if (this.assertedQueues.get(queue) === signature) return;

    const queueArguments = this.queueArguments(queueOptions);
    if (useDLQ) {
      const dlx = `${queue}_dlx`;
      const dlq = `${queue}_failed`;
      await channel.assertExchange(dlx, "direct", { durable: true });
      await channel.assertQueue(dlq, {
        durable: queueOptions.durable ?? true,
        ...(queueOptions.maxLength !== undefined && { maxLength: queueOptions.maxLength }),
        ...(queueOptions.messageTtl !== undefined && { messageTtl: queueOptions.messageTtl }),
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

  resetCache(queue?: string): void {
    if (queue) this.assertedQueues.delete(queue);
    else this.assertedQueues.clear();
  }

  isSetUp(queue: string): boolean {
    return this.assertedQueues.has(queue);
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
