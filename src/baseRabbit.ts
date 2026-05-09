import amqplib from "amqplib";
import type { Channel, ChannelModel } from "amqplib";

export abstract class BaseRabbit {
  protected connection?: ChannelModel;
  protected channel?: Channel;
  protected initPromise?: Promise<void>;

  constructor(protected readonly url: string) {}

  async init() {
    if (this.channel) return;

    if (this.initPromise) {
      await this.initPromise;
      return;
    }

    this.initPromise = (async () => {
      try {
        this.connection = await amqplib.connect(this.url);

        this.connection.on("close", () => {
          console.error("[RabbitMQ] Connection closed");
        });

        this.connection.on("error", (err) => {
          console.error("[RabbitMQ] Connection error:", err);
        });

        this.channel = await this.connection.createChannel();

        console.log(
          `[RabbitMQ] Connected to host: ${new URL(this.url).hostname}`,
        );
      } catch (error) {
        console.error("[RabbitMQ] Connection error:", error);
        throw error;
      }
    })();

    await this.initPromise;
  }

  async close() {
    await this.channel?.close();
    await this.connection?.close();

    this.channel = undefined;
    this.connection = undefined;
    this.initPromise = undefined;
  }
}
