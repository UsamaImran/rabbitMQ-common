import { BaseRabbit, type BaseRabbitOptions } from "./baseRabbit.js";
import { RabbitPublishError } from "./types.js";
import type { PublishOptions, QueueOptions } from "./types.js";

export class Producer extends BaseRabbit {
  // FIX #2: instance-level set so two Producer instances don't share state
  private assertedQueues = new Set<string>();

  constructor(url: string, options: BaseRabbitOptions = {}) {
    super(url, options);
  }

  /**
   * Publishes a message to a queue.
   * @param queue - Target queue name
   * @param message - Message payload (will be JSON.stringify'd)
   * @param publishOptions - Message-level options (persistent, expiration, priority)
   * @param queueOptions - Queue declaration options (durable, maxLength, etc.)
   * @returns Promise<boolean> - false if the socket buffer is full (call waitForDrain() before sending more)
   */
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
    } catch (err: unknown) {
      this.channel = undefined;
      const errorMessage = err instanceof Error ? err.message : String(err);
      throw new RabbitPublishError(
        `Failed to publish to queue "${queue}": ${errorMessage}`,
        queue,
        err,
      );
    }
  }

  async waitForDrain(): Promise<void> {
    const channel = await this.getChannel();
    return new Promise((resolve) => {
      channel.once("drain", () => resolve());
    });
  }

  resetQueueCache(queue?: string): void {
    if (queue) {
      this.assertedQueues.delete(queue);
    } else {
      this.assertedQueues.clear();
    }
  }
}
