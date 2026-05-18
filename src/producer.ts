import { BaseRabbit, type BaseRabbitOptions } from "./baseRabbit.js";
import { RabbitPublishError } from "./types.js";
import type { PublishOptions, QueueOptions } from "./types.js";

export class Producer extends BaseRabbit {
  // FIX #2: instance-level set so two Producer instances don't share state
  private assertedQueues = new Set<string>();

  constructor(url: string, options: BaseRabbitOptions = {}) {
    super(url, options);
  }

  async publish<T>(
    queue: string,
    message: T,
    publishOptions: PublishOptions = {},
    queueOptions: QueueOptions = {},
  ): Promise<boolean> {
    try {
      const channel = await this.getChannel();

      if (!this.assertedQueues.has(queue)) {
        await channel.assertQueue(queue, {
          durable: queueOptions.durable ?? true,
          ...(queueOptions.maxLength && { maxLength: queueOptions.maxLength }),
          ...(queueOptions.messageTtl && {
            messageTtl: queueOptions.messageTtl,
          }),
          ...(queueOptions.priority && { maxPriority: queueOptions.priority }),
        });
        this.assertedQueues.add(queue);
      }

      return channel.sendToQueue(queue, Buffer.from(JSON.stringify(message)), {
        persistent: publishOptions.persistent ?? true,
        ...(publishOptions.expiration && {
          expiration: publishOptions.expiration,
        }),
        ...(publishOptions.priority !== undefined && {
          priority: publishOptions.priority,
        }),
      });
    } catch (err) {
      this.channel = undefined;
      throw new RabbitPublishError(
        `Failed to publish to queue "${queue}": ${(err as Error).message}`,
        queue,
        err,
      );
    }
  }

  // Invalidate the asserted queue cache (useful after reconnection)
  resetQueueCache(queue?: string): void {
    if (queue) {
      this.assertedQueues.delete(queue);
    } else {
      this.assertedQueues.clear();
    }
  }
}
