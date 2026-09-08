import { ConnectionManager } from "./connectionManager.js";
import type { Channel } from "amqplib";
import type { BaseRabbitOptions, Logger } from "./types.js";

export type { BaseRabbitOptions } from "./types.js";

export abstract class BaseRabbit {
  protected channel?: Channel;
  private channelPromise?: Promise<Channel>;
  protected readonly maxRetries: number;
  protected readonly logger: Logger;
  protected readonly useDLQ: boolean;
  protected readonly queueOptions: BaseRabbitOptions["queueOptions"];

  constructor(
    protected readonly url: string,
    options: BaseRabbitOptions = {},
  ) {
    this.maxRetries = options.maxRetries ?? 5;
    this.logger = options.logger ?? console;
    this.useDLQ = options.useDLQ ?? false;
    this.queueOptions = options.queueOptions;
  }

  protected async getChannel(): Promise<Channel> {
    if (this.channel) return this.channel;
    if (this.channelPromise) return this.channelPromise;

    this.channelPromise = (async () => {
      const connection = await ConnectionManager.getConnection(
        this.url,
        this.maxRetries,
      );
      const channel = await connection.createChannel();

      const invalidate = () => {
        if (this.channel === channel) {
          this.channel = undefined;
        }
        this.onChannelInvalidated(channel);
      };

      channel.on("error", invalidate);
      channel.on("close", invalidate);

      this.channel = channel;
      return channel;
    })();

    try {
      return await this.channelPromise;
    } finally {
      this.channelPromise = undefined;
    }
  }

  /**
   * Hook for subclasses that keep channel-scoped caches.
   */
  protected onChannelInvalidated(_channel: Channel): void {
    // Intentionally empty.
  }

  async close(): Promise<void> {
    const channel = this.channel;
    this.channel = undefined;

    if (channel) {
      try {
        await channel.close();
      } catch {
        // Ignore — channel may already be closed.
      }
      this.onChannelInvalidated(channel);
    }
  }

  isConnected(): boolean {
    return ConnectionManager.isConnected(this.url);
  }

  isChannelReady(): boolean {
    return this.channel !== undefined;
  }

  getUrl(): string {
    return this.url;
  }
}
