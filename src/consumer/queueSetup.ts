import type { Channel } from "amqplib";
import type { QueueOptions } from "../types.js";

export interface QueueSetupOptions {
  useDLQ?: boolean;
  queueOptions?: QueueOptions;
}

export class QueueSetup {
  private assertedQueues = new Set<string>();

  /**
   * Setup a queue with optional DLQ configuration
   */
  async setupQueue(
    channel: Channel,
    queue: string,
    options: QueueSetupOptions = {},
  ): Promise<void> {
    const { useDLQ = false, queueOptions = {} } = options;

    // Check cache
    if (this.assertedQueues.has(queue)) {
      return;
    }

    if (useDLQ) {
      await this.setupQueueWithDLQ(channel, queue, queueOptions);
    } else {
      await channel.assertQueue(queue, {
        durable: queueOptions.durable ?? true,
        ...(queueOptions.maxLength && { maxLength: queueOptions.maxLength }),
        ...(queueOptions.messageTtl && { messageTtl: queueOptions.messageTtl }),
        ...(queueOptions.priority && { maxPriority: queueOptions.priority }),
      });
    }

    this.assertedQueues.add(queue);
  }

  /**
   * Setup queue with Dead Letter Queue
   */
  private async setupQueueWithDLQ(
    channel: Channel,
    queue: string,
    queueOptions: QueueOptions = {},
  ): Promise<void> {
    const dlx = `${queue}_dlx`;
    const dlq = `${queue}_failed`;

    // Setup DLX exchange
    await channel.assertExchange(dlx, "direct", { durable: true });

    // Setup DLQ - Use the same durable option
    await channel.assertQueue(dlq, {
      durable: queueOptions.durable ?? true,
      ...(queueOptions.maxLength && { maxLength: queueOptions.maxLength }),
      ...(queueOptions.messageTtl && { messageTtl: queueOptions.messageTtl }),
    });

    // Bind DLQ to DLX
    await channel.bindQueue(dlq, dlx, "dead-letter");

    // Setup main queue with DLQ configuration
    await channel.assertQueue(queue, {
      durable: queueOptions.durable ?? true,
      ...(queueOptions.maxLength && { maxLength: queueOptions.maxLength }),
      ...(queueOptions.messageTtl && { messageTtl: queueOptions.messageTtl }),
      ...(queueOptions.priority && { maxPriority: queueOptions.priority }),
      deadLetterExchange: dlx,
      deadLetterRoutingKey: "dead-letter",
    });
  }

  /**
   * Reset cache for a specific queue
   */
  resetCache(queue?: string): void {
    if (queue) {
      this.assertedQueues.delete(queue);
    } else {
      this.assertedQueues.clear();
    }
  }

  /**
   * Check if queue is set up
   */
  isSetUp(queue: string): boolean {
    return this.assertedQueues.has(queue);
  }
}
