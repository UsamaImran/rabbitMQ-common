import type { Channel } from "amqplib";
import type { PublishOptions } from "../types.js";

/**
 * Responsible for publishing individual messages
 */
export class MessageSender {
  /**
   * Serialize and send a single message
   */
  sendToQueue<T>(
    channel: Channel,
    queue: string,
    message: T,
    options: PublishOptions = {},
  ): boolean {
    const content = Buffer.from(JSON.stringify(message));
    const properties = {
      persistent: options.persistent ?? true,
      ...(options.expiration && { expiration: options.expiration }),
      ...(options.priority !== undefined && { priority: options.priority }),
    };

    return channel.sendToQueue(queue, content, properties);
  }

  /**
   * Serialize and publish a single message to an exchange
   */
  publishToExchange<T>(
    channel: Channel,
    exchange: string,
    routingKey: string,
    message: T,
    options: PublishOptions = {},
  ): boolean {
    const content = Buffer.from(JSON.stringify(message));
    const properties = {
      persistent: options.persistent ?? true,
      ...(options.expiration && { expiration: options.expiration }),
      ...(options.priority !== undefined && { priority: options.priority }),
    };

    return channel.publish(exchange, routingKey, content, properties);
  }
}
