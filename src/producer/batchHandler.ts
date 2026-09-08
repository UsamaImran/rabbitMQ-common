import type { Channel } from "amqplib";
import type {
  PublishOptions,
  ExchangePublishOptions,
  BatchPublishResult,
  BatchPublishError,
} from "../types.js";
import { MessageSender } from "./messageSender.js";

export class BatchHandler {
  private sender = new MessageSender();

  async publishBatch<T>(
    channel: Channel,
    queue: string,
    messages: T[],
    options: PublishOptions = {},
    waitForDrain: (channel: Channel) => Promise<void>,
  ): Promise<BatchPublishResult> {
    return this.publish(messages, (message) =>
      this.sender.sendToQueue(channel, queue, message, options),
      channel,
      waitForDrain,
    );
  }

  async publishBatchToExchange<T>(
    channel: Channel,
    exchange: string,
    routingKey: string,
    messages: T[],
    options: ExchangePublishOptions = {},
    waitForDrain: (channel: Channel) => Promise<void>,
  ): Promise<BatchPublishResult> {
    return this.publish(messages, (message) =>
      this.sender.publishToExchange(
        channel,
        exchange,
        routingKey,
        message,
        options,
      ),
      channel,
      waitForDrain,
    );
  }

  private async publish<T>(
    messages: T[],
    send: (message: T) => boolean,
    channel: Channel,
    waitForDrain: (channel: Channel) => Promise<void>,
  ): Promise<BatchPublishResult> {
    const errors: BatchPublishError[] = [];

    for (let i = 0; i < messages.length; i++) {
      try {
        // A false return means the message was accepted by the channel's
        // write buffer and the producer must wait before sending more.
        const accepted = send(messages[i]);
        if (!accepted) await waitForDrain(channel);
      } catch (err) {
        errors.push({
          index: i,
          message: messages[i],
          error: err instanceof Error ? err : new Error(String(err)),
        });
      }
    }

    return {
      total: messages.length,
      successful: messages.length - errors.length,
      failed: errors.length,
      errors,
    };
  }
}
