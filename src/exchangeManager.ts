import type { Channel } from "amqplib";
import type { ExchangeType } from "./types.js";

export class ExchangeManager {
  private channel?: Channel;
  private declaredExchanges = new Map<string, string>();

  async assertExchange(
    channel: Channel,
    exchange: string,
    type: ExchangeType,
    options: { durable?: boolean } = {},
  ): Promise<void> {
    this.resetForChannel(channel);
    const normalized = { durable: options.durable ?? true };
    const cacheKey = `${exchange}\u0000${type}`;
    const signature = JSON.stringify(normalized);

    if (this.declaredExchanges.get(cacheKey) === signature) return;

    await channel.assertExchange(exchange, type, normalized);
    this.declaredExchanges.set(cacheKey, signature);
  }

  resetForChannel(channel: Channel): void {
    if (this.channel !== channel) {
      this.channel = channel;
      this.declaredExchanges.clear();
    }
  }

  resetExchangeCache(exchange?: string, type?: ExchangeType): void {
    if (exchange && type) {
      this.declaredExchanges.delete(`${exchange}\u0000${type}`);
    } else if (exchange) {
      for (const key of this.declaredExchanges) {
        if (key.startsWith(`${exchange}\u0000`)) this.declaredExchanges.delete(key);
      }
    } else {
      this.declaredExchanges.clear();
    }
  }
}
