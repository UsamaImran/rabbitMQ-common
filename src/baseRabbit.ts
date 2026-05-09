import amqplib from "amqplib";
import type { Channel, ChannelModel } from "amqplib";

export abstract class BaseRabbit {
  protected connection?: ChannelModel;
  protected channel?: Channel;
  protected initPromise?: Promise<void>;

  constructor(protected readonly url: string) {}

  async init() {
    if (this.channel) return;

    if (this.initPromise) return this.initPromise;

    this.initPromise = this._connect();
    return this.initPromise;
  }

  private async _connect() {
    this.connection = await amqplib.connect(this.url);
    this.channel = await this.connection.createChannel();

    this.connection.on("close", () => {
      console.error("[RabbitMQ] Connection closed");
    });

    this.connection.on("error", (err) => {
      console.error("[RabbitMQ] Connection error:", err);
    });
  }
}
