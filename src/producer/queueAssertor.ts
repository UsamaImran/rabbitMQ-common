import type { Channel } from "amqplib";
import type { QueueOptions } from "../types.js";

/**
 * Responsible for asserting queues with caching
 */
export class QueueAssertor {
  private assertedQueues = new Set<string>();

  /**
   * Assert a queue exists, cache the result
   */
  async assertQueue(
    channel: Channel,
    queue: string,
    options: QueueOptions = {},
  ): Promise<void> {
    if (this.assertedQueues.has(queue)) {
      return;
    }

    await channel.assertQueue(queue, {
      durable: options.durable ?? true,
      ...(options.maxLength && { maxLength: options.maxLength }),
      ...(options.messageTtl && { messageTtl: options.messageTtl }),
      ...(options.priority && { maxPriority: options.priority }),
    });

    this.assertedQueues.add(queue);
  }

  /**
   * Reset the cache for one or all queues
   */
  resetCache(queue?: string): void {
    if (queue) {
      this.assertedQueues.delete(queue);
    } else {
      this.assertedQueues.clear();
    }
  }

  /**
   * Check if a queue has been asserted
   */
  isAsserted(queue: string): boolean {
    return this.assertedQueues.has(queue);
  }
}
