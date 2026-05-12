import { ConnectionManager } from "./connectionManager.js";
import type { Channel } from "amqplib";

export abstract class BaseRabbit {
  protected channel?: Channel;
  protected maxRetries: number = 5;

  constructor(protected readonly url: string) {}

  protected async getChannel(): Promise<Channel> {
    if (this.channel) return this.channel;

    const connection = await ConnectionManager.getConnection(
      this.url,
      this.maxRetries,
    );
    this.channel = await connection.createChannel();

    this.channel.on("error", () => {
      this.channel = undefined;
    });

    this.channel.on("close", () => {
      this.channel = undefined;
    });

    return this.channel;
  }
}
