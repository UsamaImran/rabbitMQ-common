import type { Channel } from "amqplib";
import type { ExchangeType } from "./types.js";

export class ExchangeManager {
  // Per-instance cache (same pattern as Producer.assertedQueues)
  private declaredExchanges = new Set<string>();

  async assertExchange(
    channel: Channel,
    exchange: string,
    type: ExchangeType,
    options: { durable?: boolean } = {},
  ): Promise<void> {
    const cacheKey = `${exchange}:${type}`;

    if (this.declaredExchanges.has(cacheKey)) {
      return;
    }

    await channel.assertExchange(exchange, type, {
      durable: options.durable ?? true,
    });

    this.declaredExchanges.add(cacheKey);
  }

  resetExchangeCache(exchange?: string, type?: ExchangeType): void {
    if (exchange && type) {
      this.declaredExchanges.delete(`${exchange}:${type}`);
    } else if (exchange) {
      // Remove all caches for this exchange (any type)
      for (const key of this.declaredExchanges) {
        if (key.startsWith(`${exchange}:`)) {
          this.declaredExchanges.delete(key);
        }
      }
    } else {
      this.declaredExchanges.clear();
    }
  }
}
