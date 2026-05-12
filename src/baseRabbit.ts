import { ConnectionManager } from "./connectionManager.js";
import type { Channel } from "amqplib";

export abstract class BaseRabbit {
  protected channel?: Channel;

  constructor(protected readonly url: string) {}

  protected async getChannel(): Promise<Channel> {
    if (this.channel) return this.channel;

    const connection = await ConnectionManager.getConnection(this.url);
    this.channel = await connection.createChannel();

    this.channel.on("error", () => {
      this.channel = undefined;
    });

    return this.channel;
  }

  async close(): Promise<void> {
    await this.channel?.close();
    this.channel = undefined;
  }
}
