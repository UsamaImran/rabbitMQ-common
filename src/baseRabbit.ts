import amqplib from "amqplib";
import type { Channel, ChannelModel } from "amqplib";

export abstract class BaseRabbit {
  protected connection?: ChannelModel;
  protected channel?: Channel;

  constructor(protected readonly url: string) {}

  async init() {
    try {
      this.connection = await amqplib.connect(this.url);
      this.channel = await this.connection.createChannel();

      console.log(
        `[RabbitMQ] Connected to host: ${new URL(this.url).hostname}`,
      );
    } catch (error) {
      console.error("[RabbitMQ] Connection error:", error);
      throw error;
    }
  }
}
