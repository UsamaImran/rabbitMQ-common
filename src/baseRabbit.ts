import { ConnectionManager } from "./connectionManager.js";
import type { Logger } from "./types.js";
import type { Channel } from "amqplib";

export interface BaseRabbitOptions {
  // FIX #8: maxRetries is now configurable at construction time
  maxRetries?: number;
  // NEW: pluggable logger
  logger?: Logger;
}

export abstract class BaseRabbit {
  protected channel?: Channel;
  protected maxRetries: number;
  protected logger: Logger;

  constructor(
    protected readonly url: string,
    options: BaseRabbitOptions = {},
  ) {
    this.maxRetries = options.maxRetries ?? 5;
    this.logger = options.logger ?? console;
  }

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

  // FIX #9: Exposed close() so users don't need to touch ConnectionManager directly
  async close(): Promise<void> {
    if (this.channel) {
      try {
        await this.channel.close();
      } catch {
        // ignore — channel may already be closed
      }
      this.channel = undefined;
    }
  }

  // Health check
  isConnected(): boolean {
    return ConnectionManager.isConnected(this.url);
  }
}
