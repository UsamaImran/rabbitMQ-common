import amqplib from "amqplib";
import type { Channel, ChannelModel } from "amqplib";

export abstract class BaseRabbit {
  protected connection?: ChannelModel;
  protected channel?: Channel;
  private initPromise?: Promise<void>;

  constructor(protected readonly url: string) {}

  protected async getChannel(): Promise<Channel> {
    if (this.channel) return this.channel;

    if (!this.initPromise) {
      this.initPromise = this.connect();
    }

    await this.initPromise;

    if (!this.channel) {
      throw new Error("[RabbitMQ] Channel unavailable after init");
    }

    return this.channel;
  }

  private async connect(): Promise<void> {
    try {
      this.connection = await amqplib.connect(this.url);

      this.connection.on("close", () => {
        console.error("[RabbitMQ] Connection closed");
        this.channel = undefined;
        this.connection = undefined;
        this.initPromise = undefined;
      });

      this.connection.on("error", (err) => {
        console.error("[RabbitMQ] Connection error:", err);
      });

      this.channel = await this.connection.createChannel();

      console.log(
        `[RabbitMQ] Connected to host: ${new URL(this.url).hostname}`,
      );
    } catch (error) {
      this.initPromise = undefined;
      console.error("[RabbitMQ] Failed to connect:", error);
      throw error;
    }
  }

  async close(): Promise<void> {
    await this.channel?.close();
    await this.connection?.close();

    this.channel = undefined;
    this.connection = undefined;
    this.initPromise = undefined;
  }
}
